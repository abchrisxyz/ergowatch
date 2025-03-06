use itertools::Itertools;
use std::collections::HashMap;

use super::constants::BANK_NFT;
use super::constants::CONTRACT_ADDRESS_ID;
use super::constants::CONTRACT_CREATION_HEIGHT;
use super::constants::NETWORK_FEE_ADDRESS_ID;
use super::constants::ORACLE_NFT;
use super::constants::RC_ASSET_ID;
use super::constants::SC_ASSET_ID;
use super::types::BankTransaction;
use super::types::Batch;
use super::types::DailyOHLC;
use super::types::Event;
use super::types::HistoryRecord;
use super::types::MonthlyOHLC;
use super::types::NoopBankTransaction;
use super::types::OraclePosting;
use super::types::ServiceStats;
use super::types::WeeklyOHLC;
use crate::core::types::AddressID;
use crate::core::types::Block;
use crate::core::types::BoxData;
use crate::core::types::CoreData;
use crate::core::types::Digest32;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;
use crate::core::types::Transaction;
use crate::framework::StampedData;
use crate::workers::sigmausd::types::OHLCGroup;

pub struct Parser {
    // Schema state is initialized at creation,
    // so cache data is guaranteed to be present.
    cache: ParserCache,
}

pub struct ParserCache {
    // Number of bank transactions so far (used to derive bank tx indices)
    pub bank_transaction_count: i32,
    // Last history record
    pub last_history_record: HistoryRecord,
    // Last OHLC records
    pub last_ohlc_group: OHLCGroup,
}

impl Parser {
    pub fn new(cache: ParserCache) -> Self {
        Self { cache }
    }

    pub(super) fn extract_batch(
        &mut self,
        stamped_data: &StampedData<CoreData>,
    ) -> StampedData<Batch> {
        let block = &stamped_data.data.block;
        assert!(block.header.height > CONTRACT_CREATION_HEIGHT);

        // Extract events from block transactions
        let events = extract_events(block, self.cache.bank_transaction_count);

        // Convert events to history records
        let history_records = generate_history_records(&events, &self.cache.last_history_record);

        // Derive a SigRSV for each history records
        let rc_prices: Vec<NanoERG> = history_records.iter().map(|r| r.rc_price()).collect();

        let history_record = history_records.last().map(|hr| hr.clone());

        // OHLC's
        let (daily_ohlc_records, weekly_ohlc_records, monthly_ohlc_records) = generate_ohlc_records(
            block.header.timestamp,
            &rc_prices,
            &self.cache.last_ohlc_group,
        );

        // Services
        let service_diffs = extract_service_diffs(block.header.timestamp, &events);

        // Update cached history record
        if let Some(hr) = &history_record {
            self.cache.last_history_record = hr.clone();
        }
        // Update cached bank tx count
        let n_bank_txs = events
            .iter()
            .filter(|r| matches!(r, Event::BankTx(_)))
            .count() as i32;
        self.cache.bank_transaction_count += n_bank_txs;
        // Update cached OHLC records
        if let Some(daily) = daily_ohlc_records.last() {
            self.cache.last_ohlc_group.daily = daily.clone();
        }
        if let Some(weekly) = weekly_ohlc_records.last() {
            self.cache.last_ohlc_group.weekly = weekly.clone();
        }
        if let Some(monthly) = monthly_ohlc_records.last() {
            self.cache.last_ohlc_group.monthly = monthly.clone();
        }

        // Pack new batch
        stamped_data.wrap(Batch {
            events,
            history_record,
            daily_ohlc_records,
            weekly_ohlc_records,
            monthly_ohlc_records,
            service_diffs,
        })
    }
}

fn extract_events(block: &Block, bank_tx_count: i32) -> Vec<Event> {
    let mut local_bank_tx_count = bank_tx_count;
    let height = block.header.height;
    let timestamp = block.header.timestamp;
    block
        .transactions
        .iter()
        .filter_map(|tx| extract_event(tx, height, timestamp, &mut local_bank_tx_count))
        .collect()
}

/// Extracts an event from the transaction, if any.
fn extract_event(
    tx: &Transaction,
    height: Height,
    timestamp: Timestamp,
    bank_tx_count: &mut i32,
) -> Option<Event> {
    // Look for presence of bank box in outputs
    if tx_has_bank_box(tx) {
        let bank_tx = extract_bank_tx(tx, height, timestamp, bank_tx_count);
        if bank_tx.reserves_diff == 0 {
            Some(Event::NoopBankTx(NoopBankTransaction {
                height: bank_tx.height,
                tx_idx: tx.index,
                tx_id: tx.id.clone(),
                box_id: bank_tx.box_id,
            }))
        } else {
            Some(Event::BankTx(bank_tx))
        }
    } else if tx_has_oracle_prep_box(tx) {
        Some(Event::Oracle(extract_oracle_posting(tx, height)))
    } else {
        None
    }
}

fn tx_has_bank_box(tx: &Transaction) -> bool {
    tx.outputs.iter().any(|o| {
        o.address_id == CONTRACT_ADDRESS_ID && o.assets.iter().any(|a| a.asset_id == BANK_NFT)
    })
}

/// Determine if transaction produces an oracle prep box.
fn tx_has_oracle_prep_box(tx: &Transaction) -> bool {
    // Can't rely on fixed prep box address, as subject to change (contract updates)
    // Instead, go with following ruleset:
    //  - prep box must hold oracle NFT (others can too at times)
    //  - prep box must have R4 and R5 set, not R6.
    //  - prep box is minted in a tx collecting data inputs
    !tx.data_inputs.is_empty()
        && tx.outputs.iter().any(|o| {
            o.assets.iter().any(|a| a.asset_id == ORACLE_NFT)
                && o.additional_registers.has_r4()
                && o.additional_registers.has_r5()
                && !o.additional_registers.has_r6()
        })
}

/// Build a bank transaction from a tx known to contain a bank box
fn extract_bank_tx(
    tx: &Transaction,
    height: Height,
    timestamp: Timestamp,
    bank_tx_count: &mut i32,
) -> BankTransaction {
    // New bank box id
    let bank_outputs: Vec<&BoxData> = tx
        .outputs
        .iter()
        .filter(|o| {
            o.address_id == CONTRACT_ADDRESS_ID && o.assets.iter().any(|a| a.asset_id == BANK_NFT)
        })
        .collect();
    assert_eq!(bank_outputs.len(), 1);
    let box_id = bank_outputs[0].box_id.clone();

    // Get net balance diffs
    let mut erg_diffs: HashMap<AddressID, NanoERG> = HashMap::new();
    let mut sc_diffs: HashMap<AddressID, NanoERG> = HashMap::new();
    let mut rc_diffs: HashMap<AddressID, NanoERG> = HashMap::new();

    // Loop over output boxes, adding to balances
    for output in &tx.outputs {
        erg_diffs
            .entry(output.address_id)
            .and_modify(|v| *v += output.value)
            .or_insert(output.value);
        let sc_amount: i64 = output
            .assets
            .iter()
            .filter(|a| a.asset_id == SC_ASSET_ID)
            .map(|a| a.amount)
            .sum();
        let rc_amount: i64 = output
            .assets
            .iter()
            .filter(|a| a.asset_id == RC_ASSET_ID)
            .map(|a| a.amount)
            .sum();
        if sc_amount != 0 {
            sc_diffs
                .entry(output.address_id)
                .and_modify(|v| *v += sc_amount)
                .or_insert(sc_amount);
        }
        if rc_amount != 0 {
            rc_diffs
                .entry(output.address_id)
                .and_modify(|v| *v += rc_amount)
                .or_insert(rc_amount);
        }
    }

    // Loop over input boxes, taking from balances
    for input in &tx.inputs {
        erg_diffs
            .entry(input.address_id)
            .and_modify(|v| *v -= input.value)
            .or_insert(-input.value);
        let sc_amount: i64 = input
            .assets
            .iter()
            .filter(|a| a.asset_id == SC_ASSET_ID)
            .map(|a| a.amount)
            .sum();
        let rc_amount: i64 = input
            .assets
            .iter()
            .filter(|a| a.asset_id == RC_ASSET_ID)
            .map(|a| a.amount)
            .sum();
        if sc_amount != 0 {
            sc_diffs
                .entry(input.address_id)
                .and_modify(|v| *v -= sc_amount)
                .or_insert(-sc_amount);
        }
        if rc_amount != 0 {
            rc_diffs
                .entry(input.address_id)
                .and_modify(|v| *v -= rc_amount)
                .or_insert(-rc_amount);
        }
    }

    // Supply diffs
    let reserves_diff: i64 = *erg_diffs
        .get(&CONTRACT_ADDRESS_ID)
        .expect("no erg balance diff for sigmausd contract");
    let circ_sc_diff = -*sc_diffs.get(&CONTRACT_ADDRESS_ID).unwrap_or(&0);
    let circ_rc_diff = -*rc_diffs.get(&CONTRACT_ADDRESS_ID).unwrap_or(&0);

    let (service_fee, service_address_id) = if reserves_diff > 0 {
        // Minting tx
        assert!(circ_sc_diff > 0 || circ_rc_diff > 0);
        extract_service_from_minting_tx_diffs(&erg_diffs, &sc_diffs, &rc_diffs, &tx.id)
    } else if reserves_diff < 0 {
        // Redeeming tx
        assert!(circ_sc_diff < 0 || circ_rc_diff < 0);
        extract_service_from_redeeming_tx_diffs(&erg_diffs, &tx)
    } else {
        // No-op, possible bank update
        assert!(circ_rc_diff == 0 && circ_rc_diff == 0);
        (0, None)
    };

    // Now build the event
    *bank_tx_count += 1;
    BankTransaction {
        index: *bank_tx_count,
        height,
        timestamp,
        reserves_diff,
        circ_sc_diff,
        circ_rc_diff,
        box_id,
        service_fee,
        service_address_id,
    }
}

fn extract_service_from_minting_tx_diffs(
    erg_diffs: &HashMap<AddressID, NanoERG>,
    sc_diffs: &HashMap<AddressID, NanoERG>,
    rc_diffs: &HashMap<AddressID, NanoERG>,
    tx_id: &Digest32,
) -> (NanoERG, Option<AddressID>) {
    // Assuming any address receiving erg only could be a service provider.
    // Works for minting txs, not for redeeming ones.
    let service_candidates: Vec<&AddressID> = erg_diffs
        .keys()
        // Exclude bank and fee addresses
        .filter(|ai| **ai != CONTRACT_ADDRESS_ID && **ai != NETWORK_FEE_ADDRESS_ID)
        // Exclude any addresses involved with SC/RC tokens
        .filter(|ai| !sc_diffs.contains_key(ai))
        .filter(|ai| !rc_diffs.contains_key(ai))
        // Keep addresses credited with erg.
        // Key is guaranteed to exists because we're looping over them.
        .filter(|ai| erg_diffs[ai] > 0)
        .collect();

    match service_candidates.len() {
        // Direct interaction - no service involved.
        0 => (0, None),
        // This looks like a service.
        1 => {
            let ai = service_candidates[0];
            let fee = erg_diffs[ai];
            (fee, Some(*ai))
        }
        // Unnable to tell what's going on here - log and ignore.
        _ => {
            tracing::warn!(
                "multiple service candidates in minting transaction {}",
                tx_id
            );
            (0, None)
        }
    }
}

fn extract_service_from_redeeming_tx_diffs(
    erg_diffs: &HashMap<AddressID, i64>,
    tx: &Transaction,
) -> (NanoERG, Option<AddressID>) {
    // List unique output address id's
    let service_candidates: Vec<AddressID> = tx
        .outputs
        .iter()
        .map(|output| output.address_id)
        .filter(|ai| *ai != CONTRACT_ADDRESS_ID)
        .filter(|ai| *ai != NETWORK_FEE_ADDRESS_ID)
        .unique()
        .collect();

    // TODO: check redeemed amount vs network fee (+potential service fee) ?

    match service_candidates.len() {
        0 => {
            // Could be empty if no service and redeemed amount == tx fee...
            (0, None)
        }
        1 => {
            // Only one address in outputs, so can't be a service.
            // Or corner case where redeemed amount = service + tx fee,
            // which we might as well ignore.
            (0, None)
        }
        2 => {
            // Two candidates - assume smallest diff is service fee
            let ai0 = service_candidates[0];
            let ai1 = service_candidates[1];
            let diff0 = erg_diffs[&ai0];
            let diff1 = erg_diffs[&ai1];
            assert!(diff1 > 0);

            // Label both addresses as lo/hi depending on their net diff.
            // lo is the one with the smaller diff,
            // hi is the other one.
            let (lo, hi) = if diff0 < diff1 {
                ((diff0, ai0), (diff1, ai1))
            } else if diff0 > diff1 {
                ((diff1, ai1), (diff0, ai0))
            } else {
                // Same net diffs, so can't tell them appart.
                // Could be resolved by checking if one of them was flagged as a service before.
                // Ignoring for now as it is rare and low impact.
                tracing::warn!(
                    "ignoring two service candidates with equal balance diffs in tx {}",
                    tx.id
                );
                return (0, None);
            };

            // Highest diff expected to be always positive
            assert!(hi.0 > 0);

            // Need to check lowest diff sign as it could be negative
            // if the redeemed amount doesn't cover fees.
            if lo.0 > 0 {
                // In most casese the lowest diff is positive and we assume it is the service fee.
                (lo.0, Some(lo.1))
            } else {
                // But sometimes, when redeemed amount is less than fees, lowest diff is negative
                // and so the other address (which has a positive diff) must be the service.
                (hi.0, Some(hi.1))
            }
        }
        _ => {
            // More than two candidates, can't tell them apart - log and ignore
            tracing::warn!(
                "multiple service candidates in redeeming transaction {}",
                tx.id
            );
            (0, None)
        }
    }
}

/// Build an oracle posting from a tx known to contain an oracle prep box
fn extract_oracle_posting(tx: &Transaction, height: Height) -> OraclePosting {
    // Find the new prep box
    let prep_boxes: Vec<&BoxData> = tx
        .outputs
        .iter()
        .filter(|o| o.assets.iter().any(|a| a.asset_id == ORACLE_NFT))
        .collect();
    assert_eq!(prep_boxes.len(), 1);
    let prep_box: &BoxData = prep_boxes[0];

    // Read datapoint
    let datapoint: i64 = match prep_box.additional_registers.r4() {
        Some(register) => register.rendered_value.parse::<i64>().unwrap(),
        None => panic!("expected R4 for oracle prep box"),
    };

    OraclePosting {
        height,
        datapoint,
        box_id: prep_box.box_id.clone(),
    }
}

/// Derive new records by applying `events` to `last` one.
fn generate_history_records(events: &Vec<Event>, last: &HistoryRecord) -> Vec<HistoryRecord> {
    if events.is_empty() {
        return vec![];
    }
    let mut records: Vec<HistoryRecord> = vec![];
    let mut prev = last;
    for event in events {
        let mut new = prev.clone();
        match event {
            Event::Oracle(oracle_posting) => {
                new.height = oracle_posting.height;
                new.oracle = oracle_posting.datapoint;
            }
            Event::BankTx(bank_tx) => {
                new.height = bank_tx.height;
                new.circ_sc += bank_tx.circ_sc_diff;
                new.circ_rc += bank_tx.circ_rc_diff;
                new.reserves += bank_tx.reserves_diff;
                // Assuming no txs will ever modify both SC and RC.
                // Might be better to use oracle and rsv price instead.
                if bank_tx.circ_sc_diff != 0 {
                    assert_eq!(bank_tx.circ_rc_diff, 0);
                    new.sc_net += bank_tx.reserves_diff;
                } else if bank_tx.circ_rc_diff != 0 {
                    assert_eq!(bank_tx.circ_sc_diff, 0);
                    new.rc_net += bank_tx.reserves_diff;
                }
            }
            Event::NoopBankTx(_) => {
                continue;
            }
        }
        records.push(new);
        prev = records.last().unwrap();
    }
    records
}

/// Generate any new OHLC records.
fn generate_ohlc_records(
    timestamp: Timestamp,
    prices: &Vec<NanoERG>,
    last: &OHLCGroup,
) -> (Vec<DailyOHLC>, Vec<WeeklyOHLC>, Vec<MonthlyOHLC>) {
    // New records are needed if either of two things happen:
    // - current block has any history records
    // - current block's timestamp opens a new window
    if prices.is_empty() {
        // No price changes in this block.
        // Just propagate last close to any new windows.
        let date = DailyOHLC::date_from_timestamp(timestamp);
        return (
            last.daily.propagate_to(&date),
            last.weekly.propagate_to(&date),
            last.monthly.propagate_to(&date),
        );
    }
    // At least one price, so safe to unwrap.
    let block_daily = DailyOHLC::from_prices(timestamp, &prices).unwrap();
    let block_weekly = WeeklyOHLC::from_daily(&block_daily);
    let block_monthly = MonthlyOHLC::from_daily(&block_daily);

    (
        block_daily.fill_since(&last.daily),
        block_weekly.fill_since(&last.weekly),
        block_monthly.fill_since(&last.monthly),
    )
}

// TODO: include None for direct txs
fn extract_service_diffs(timestamp: Timestamp, events: &Vec<Event>) -> Vec<ServiceStats> {
    let mut diffs = vec![];
    for event in events {
        if let Event::BankTx(btx) = event {
            diffs.push(ServiceStats {
                address_id: btx.service_address_id,
                tx_count: 1,
                first_tx: timestamp,
                last_tx: timestamp,
                fees: btx.service_fee.into(),
                volume: btx.reserves_diff.abs().into(),
            });
        }
    }
    diffs
}

#[cfg(test)]
mod tests {
    use super::super::types::MonthlyOHLC;
    use super::super::types::WeeklyOHLC;
    use super::*;
    use crate::core::types::Transaction;
    use rust_decimal::Decimal;
    use time::macros::date;

    #[test]
    fn test_extract_event_nothing() {
        let input = BoxData::dummy()
            .address_id(AddressID::dummy(12))
            .value(1000);
        let output0 = BoxData::dummy().address_id(AddressID::dummy(13)).value(900);
        let output1 = BoxData::dummy().address_id(AddressID::dummy(5)).value(100);

        let tx = Transaction::dummy()
            .add_input(input)
            .add_output(output0)
            .add_output(output1);

        let height = 600;
        let timestamp = 123456789;
        let mut bank_tx_count = 5;
        let event = extract_event(&tx, height, timestamp, &mut bank_tx_count);
        assert!(event.is_none());
        assert_eq!(bank_tx_count, 5);
    }

    #[test]
    fn test_extract_event_contract_but_not_bank() {
        let input = BoxData::dummy()
            .address_id(AddressID::dummy(12))
            .value(1000);
        let output0 = BoxData::dummy().address_id(CONTRACT_ADDRESS_ID).value(900);
        let output1 = BoxData::dummy().address_id(AddressID::dummy(5)).value(100);

        let tx = Transaction::dummy()
            .add_input(input)
            .add_output(output0)
            .add_output(output1);

        let height = 600;
        let timestamp = 123456789;
        let mut bank_tx_count = 5;
        let event = extract_event(&tx, height, timestamp, &mut bank_tx_count);
        assert!(event.is_none());
        assert_eq!(bank_tx_count, 5);
    }

    #[test]
    fn test_extract_event_sc_mint_direct() {
        // User mints 200 SigUSD for 100 nanoERG
        let user: AddressID = AddressID::dummy(12345);
        let bank_input = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(SC_ASSET_ID, 500);
        let bank_output = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(SC_ASSET_ID, 300);
        let user_input = BoxData::dummy().address_id(user).value(5000);
        let user_output = BoxData::dummy()
            .address_id(user)
            .value(4900)
            .add_asset(SC_ASSET_ID, 200);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output);

        let height = 600;
        let timestamp = 123456789;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, timestamp, &mut new_bank_tx_count).unwrap() {
            Event::BankTx(btx) => {
                assert_eq!(btx.height, height);
                assert_eq!(btx.timestamp, timestamp);
                assert_eq!(btx.box_id, tx.outputs[0].box_id);
                assert_eq!(btx.reserves_diff, 100);
                assert_eq!(btx.circ_sc_diff, 200);
                assert_eq!(btx.circ_rc_diff, 0);
                assert_eq!(btx.service_fee, 0);
                assert_eq!(btx.service_address_id, None);
            }
            _ => {
                panic!("fail")
            }
        }
        assert_eq!(new_bank_tx_count, bank_tx_count + 1);
    }

    #[test]
    fn test_extract_event_rc_mint_with_service() {
        // User mints 200 SigRSV for 100 nanoERG
        let user: AddressID = AddressID::dummy(12345);
        let service: AddressID = AddressID::dummy(6789);
        let bank_input = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 500);
        let bank_output = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 300);
        let user_input = BoxData::dummy().address_id(user).value(5103);
        let user_output = BoxData::dummy()
            .address_id(user)
            .value(5000)
            .add_asset(RC_ASSET_ID, 200);
        let service_output = BoxData::dummy().address_id(service).value(2);
        let fee_output = BoxData::dummy().address_id(NETWORK_FEE_ADDRESS_ID).value(1);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output)
            .add_output(service_output)
            .add_output(fee_output);

        let height = 600;
        let timestamp = 123456789;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, timestamp, &mut new_bank_tx_count).unwrap() {
            Event::BankTx(btx) => {
                assert_eq!(btx.height, height);
                assert_eq!(btx.box_id, tx.outputs[0].box_id);
                assert_eq!(btx.reserves_diff, 100);
                assert_eq!(btx.circ_sc_diff, 0);
                assert_eq!(btx.circ_rc_diff, 200);
                assert_eq!(btx.service_fee, 2);
                assert_eq!(btx.service_address_id, Some(service));
            }
            _ => {
                panic!("fail")
            }
        }
        assert_eq!(new_bank_tx_count, bank_tx_count + 1);
    }

    #[test]
    fn test_extract_event_rc_mint_to_different_address_with_service() {
        // User mints 200 SigRSV for 100 nanoERG.
        // User input address can be different from user output address.
        let user_send: AddressID = AddressID::dummy(123451);
        let user_recv: AddressID = AddressID::dummy(123452);
        let service: AddressID = AddressID::dummy(6789);
        let bank_input = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 500);
        let bank_output = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 300);
        let user_input = BoxData::dummy().address_id(user_send).value(5103);
        let user_output = BoxData::dummy()
            .address_id(user_recv)
            .value(5000)
            .add_asset(RC_ASSET_ID, 200);
        let service_output = BoxData::dummy().address_id(service).value(2);
        let fee_output = BoxData::dummy().address_id(NETWORK_FEE_ADDRESS_ID).value(1);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output)
            .add_output(service_output)
            .add_output(fee_output);

        let height = 600;
        let timestamp = 123456789;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, timestamp, &mut new_bank_tx_count).unwrap() {
            Event::BankTx(btx) => {
                assert_eq!(btx.height, height);
                assert_eq!(btx.box_id, tx.outputs[0].box_id);
                assert_eq!(btx.reserves_diff, 100);
                assert_eq!(btx.circ_sc_diff, 0);
                assert_eq!(btx.circ_rc_diff, 200);
                assert_eq!(btx.service_fee, 2);
                assert_eq!(btx.service_address_id, Some(service));
            }
            _ => {
                panic!("fail")
            }
        }
        assert_eq!(new_bank_tx_count, bank_tx_count + 1);
    }

    #[test]
    fn test_extract_event_sc_redeem_with_multiple_service_candidates() {
        // User redeems 200 SigUSD for 100 nanoERG
        let user: AddressID = AddressID::dummy(12345);
        let bank_input = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(SC_ASSET_ID, 500);
        let bank_output = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(SC_ASSET_ID, 700);
        let user_input = BoxData::dummy()
            .address_id(user)
            .value(5005)
            .add_asset(SC_ASSET_ID, 200);
        let user_output = BoxData::dummy().address_id(user).value(5100);
        let other1_output = BoxData::dummy()
            .address_id(AddressID::dummy(30000))
            .value(2);
        let other2_output = BoxData::dummy()
            .address_id(AddressID::dummy(40000))
            .value(3);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output)
            .add_output(other1_output)
            .add_output(other2_output);

        let height = 600;
        let timestamp = 123456789;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, timestamp, &mut new_bank_tx_count).unwrap() {
            Event::BankTx(btx) => {
                assert_eq!(btx.height, height);
                assert_eq!(btx.box_id, tx.outputs[0].box_id);
                assert_eq!(btx.reserves_diff, -100);
                assert_eq!(btx.circ_sc_diff, -200);
                assert_eq!(btx.circ_rc_diff, 0);
                assert_eq!(btx.service_fee, 0);
                assert_eq!(btx.service_address_id, None);
            }
            _ => {
                panic!("fail")
            }
        }
        assert_eq!(new_bank_tx_count, bank_tx_count + 1);
    }

    #[test]
    fn test_extract_event_rc_redeem_to_same_address_direct() {
        // User redeems 200 SigRSV for 100 nanoERG
        let user: AddressID = AddressID::dummy(12345);
        let bank_input = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 500);
        let bank_output = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 700);
        let user_input = BoxData::dummy()
            .address_id(user)
            .value(5000)
            .add_asset(RC_ASSET_ID, 200);
        let user_output = BoxData::dummy().address_id(user).value(5100);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output);

        let height = 600;
        let timestamp = 123456789;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, timestamp, &mut new_bank_tx_count).unwrap() {
            Event::BankTx(btx) => {
                assert_eq!(btx.height, height);
                assert_eq!(btx.box_id, tx.outputs[0].box_id);
                assert_eq!(btx.reserves_diff, -100);
                assert_eq!(btx.circ_sc_diff, 0);
                assert_eq!(btx.circ_rc_diff, -200);
                assert_eq!(btx.service_fee, 0);
                assert_eq!(btx.service_address_id, None);
            }
            _ => {
                panic!("fail")
            }
        }
        assert_eq!(new_bank_tx_count, bank_tx_count + 1);
    }

    #[test]
    fn test_extract_event_rc_redeem_to_different_address_direct() {
        // User redeems 200 SigRSV for 100 nanoERG
        let user_send: AddressID = AddressID::dummy(123451);
        let user_recv: AddressID = AddressID::dummy(123452);
        let bank_input = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 500);
        let bank_output = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 700);
        let user_input = BoxData::dummy()
            .address_id(user_send)
            .value(5000)
            .add_asset(RC_ASSET_ID, 200);
        let user_output = BoxData::dummy().address_id(user_recv).value(5100);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output);

        let height = 600;
        let timestamp = 123456789;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, timestamp, &mut new_bank_tx_count).unwrap() {
            Event::BankTx(btx) => {
                assert_eq!(btx.height, height);
                assert_eq!(btx.box_id, tx.outputs[0].box_id);
                assert_eq!(btx.reserves_diff, -100);
                assert_eq!(btx.circ_sc_diff, 0);
                assert_eq!(btx.circ_rc_diff, -200);
                assert_eq!(btx.service_fee, 0);
                assert_eq!(btx.service_address_id, None);
            }
            _ => {
                panic!("fail")
            }
        }
        assert_eq!(new_bank_tx_count, bank_tx_count + 1);
    }

    #[test]
    fn test_extract_event_rc_redeem_to_different_address_with_service() {
        // User redeems 200 SigRSV for 100 nanoERG
        let user_send: AddressID = AddressID::dummy(123451);
        let user_recv: AddressID = AddressID::dummy(123452);
        let service: AddressID = AddressID::dummy(6789);
        let bank_input = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 500);
        let bank_output = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 700);
        let user_input = BoxData::dummy()
            .address_id(user_send)
            .value(5000)
            .add_asset(RC_ASSET_ID, 200);
        let user_output = BoxData::dummy().address_id(user_recv).value(5097);
        let service_output = BoxData::dummy().address_id(service).value(2);
        let fee_output = BoxData::dummy().address_id(NETWORK_FEE_ADDRESS_ID).value(1);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output)
            .add_output(service_output)
            .add_output(fee_output);

        let height = 600;
        let timestamp = 123456789;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, timestamp, &mut new_bank_tx_count).unwrap() {
            Event::BankTx(btx) => {
                assert_eq!(btx.height, height);
                assert_eq!(btx.box_id, tx.outputs[0].box_id);
                assert_eq!(btx.reserves_diff, -100);
                assert_eq!(btx.circ_sc_diff, 0);
                assert_eq!(btx.circ_rc_diff, -200);
                assert_eq!(btx.service_fee, 2);
                assert_eq!(btx.service_address_id, Some(service));
            }
            _ => {
                panic!("fail")
            }
        }
        assert_eq!(new_bank_tx_count, bank_tx_count + 1);
    }

    #[test]
    fn test_extract_event_rc_redeem_less_than_fee_to_same_address_direct() {
        // User redeems 1 SigRSV for 1 nanoERG with 9 nanoERG tx fee
        let user: AddressID = AddressID::dummy(12345);
        let bank_input = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1001)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 500);
        let bank_output = BoxData::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_ASSET_ID, 501);
        let user_input = BoxData::dummy()
            .address_id(user)
            .value(5010)
            .add_asset(RC_ASSET_ID, 1);
        let user_output = BoxData::dummy().address_id(user).value(5000);
        let fee_output = BoxData::dummy().address_id(NETWORK_FEE_ADDRESS_ID).value(9);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output)
            .add_output(fee_output);

        let height = 600;
        let timestamp = 123456789;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, timestamp, &mut new_bank_tx_count).unwrap() {
            Event::BankTx(btx) => {
                assert_eq!(btx.height, height);
                assert_eq!(btx.box_id, tx.outputs[0].box_id);
                assert_eq!(btx.reserves_diff, -1);
                assert_eq!(btx.circ_sc_diff, 0);
                assert_eq!(btx.circ_rc_diff, -1);
                assert_eq!(btx.service_fee, 0);
                assert_eq!(btx.service_address_id, None);
            }
            _ => {
                panic!("fail")
            }
        }
        assert_eq!(new_bank_tx_count, bank_tx_count + 1);
    }

    #[test]
    fn test_extract_event_oracle_posting() {
        // Actual prep tx would be spending a live epoch box,
        // but we don't rely on that so can just use a dummy.
        let dummy_input = BoxData::dummy();
        let prep_output = BoxData::dummy()
            .add_asset(ORACLE_NFT, 1)
            .set_registers(r#"{"R4": "05baafd2a302", "R5": "04bca7b201"}"#);

        let tx = Transaction::dummy()
            .add_input(dummy_input)
            .add_data_input(BoxData::dummy())
            .add_output(prep_output);

        let height = 600;
        let timestamp = 123456789;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, timestamp, &mut new_bank_tx_count).unwrap() {
            Event::Oracle(posting) => {
                assert_eq!(posting.height, height);
                assert_eq!(posting.datapoint, 305810397);
                assert_eq!(posting.box_id, tx.outputs[0].box_id);
            }
            _ => {
                panic!("fail")
            }
        }
        assert_eq!(new_bank_tx_count, bank_tx_count);
    }

    #[test]
    fn test_history_no_events() {
        let last = HistoryRecord {
            height: 1000,
            oracle: 305810397,
            circ_sc: 1000,
            circ_rc: 50000,
            reserves: 1000,
            sc_net: 500,
            rc_net: 2000,
        };
        let events = vec![];
        let records = generate_history_records(&events, &last);
        assert!(records.is_empty());
    }

    #[test]
    fn test_history_with_bank_event() {
        let last = HistoryRecord {
            height: 1000,
            oracle: 305810397,
            circ_sc: 1000,
            circ_rc: 50000,
            reserves: 1000,
            sc_net: 500,
            rc_net: 2000,
        };
        let events = vec![Event::BankTx(BankTransaction {
            index: 5,
            height: 1020,
            timestamp: 123456789,
            reserves_diff: 100,
            circ_sc_diff: 200,
            circ_rc_diff: 0,
            box_id: "dummy".to_string(),
            service_fee: 0,
            service_address_id: None,
        })];
        let records = generate_history_records(&events, &last);
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert!(rec.height == 1020);
        assert!(rec.oracle == last.oracle);
        assert!(rec.circ_sc == last.circ_sc + 200);
        assert!(rec.circ_rc == last.circ_rc);
        assert!(rec.reserves == last.reserves + 100);
        assert!(rec.sc_net == last.sc_net + 100);
        assert!(rec.rc_net == last.rc_net);
    }

    #[test]
    fn test_history_with_oracle_event() {
        let last = HistoryRecord {
            height: 1000,
            oracle: 305810397,
            circ_sc: 1000,
            circ_rc: 50000,
            reserves: 1000,
            sc_net: 500,
            rc_net: 2000,
        };
        let events = vec![Event::Oracle(OraclePosting {
            height: 1020,
            datapoint: 305810397 + 1,
            box_id: "dummy".to_string(),
        })];
        let records = generate_history_records(&events, &last);
        assert_eq!(records.len(), 1);
        let rec = &records[0];
        assert!(rec.height == 1020);
        assert!(rec.oracle == last.oracle + 1);
        assert!(rec.circ_sc == last.circ_sc);
        assert!(rec.circ_rc == last.circ_rc);
        assert!(rec.reserves == last.reserves);
        assert!(rec.sc_net == last.sc_net);
        assert!(rec.rc_net == last.rc_net);
    }

    impl ParserCache {
        pub fn dummy() -> Self {
            let hr = HistoryRecord {
                height: 1000,
                oracle: 305810397,
                circ_sc: 1000,
                circ_rc: 50000,
                reserves: 1000,
                sc_net: 500,
                rc_net: 2000,
            };
            let og = OHLCGroup {
                // Wednesday 13 Jul
                daily: DailyOHLC::dummy().date(time::macros::date!(2021 - 07 - 15)),
                // Monday of the week
                weekly: WeeklyOHLC::dummy().date(time::macros::date!(2021 - 07 - 11)),
                // First of the month
                monthly: MonthlyOHLC::dummy().date(time::macros::date!(2021 - 07 - 01)),
            };
            Self {
                bank_transaction_count: 123,
                last_history_record: hr,
                last_ohlc_group: og,
            }
        }
    }

    #[test]
    fn test_parser_cache_no_events() {
        let cache = ParserCache::dummy();
        let mut parser = Parser::new(cache);
        let data = CoreData {
            block: Block::dummy()
                .height(533_000)
                .timestamp(1626396302125) // 2021-07-16
                .add_tx(
                    Transaction::dummy()
                        .add_input(BoxData::dummy().value(1000))
                        .add_output(BoxData::dummy().value(1000)),
                )
                .add_tx(Transaction::dummy()),
        };
        // Check state before
        assert_eq!(parser.cache.bank_transaction_count, 123);
        assert_eq!(parser.cache.last_history_record.height, 1000);
        assert_eq!(
            parser.cache.last_ohlc_group.daily.0.t,
            date!(2021 - 07 - 15)
        );

        let _batch = parser.extract_batch(&data.into());

        // Check state after
        assert_eq!(parser.cache.bank_transaction_count, 123);
        assert_eq!(parser.cache.last_history_record.height, 1000);
        assert_eq!(
            parser.cache.last_ohlc_group.daily.0.t,
            date!(2021 - 07 - 16)
        );
    }

    #[test]
    fn test_parser_cache_with_event() {
        let cache = ParserCache::dummy();
        let mut parser = Parser::new(cache);
        let user: AddressID = AddressID::dummy(12345);
        let data = CoreData {
            block: Block::dummy()
                .height(533_000)
                .timestamp(1626396302125) // 2021-07-16
                .add_tx(
                    Transaction::dummy()
                        // bank input
                        .add_input(
                            BoxData::dummy()
                                .address_id(CONTRACT_ADDRESS_ID)
                                .value(1000)
                                .add_asset(BANK_NFT, 1)
                                .add_asset(SC_ASSET_ID, 500),
                        )
                        // user input
                        .add_input(BoxData::dummy().address_id(user).value(5000))
                        // bank output
                        .add_output(
                            BoxData::dummy()
                                .address_id(CONTRACT_ADDRESS_ID)
                                .value(1100)
                                .add_asset(BANK_NFT, 1)
                                .add_asset(SC_ASSET_ID, 300),
                        )
                        // user output
                        .add_output(
                            BoxData::dummy()
                                .address_id(user)
                                .value(4900)
                                .add_asset(SC_ASSET_ID, 200),
                        ),
                )
                .add_tx(Transaction::dummy()),
        };
        // Check state before
        assert_eq!(parser.cache.bank_transaction_count, 123);
        assert_eq!(parser.cache.last_history_record.height, 1000);
        assert_eq!(
            parser.cache.last_ohlc_group.daily.0.t,
            date!(2021 - 07 - 15)
        );

        let _batch = parser.extract_batch(&data.into());

        // Check state after
        assert_eq!(parser.cache.bank_transaction_count, 124);
        assert_eq!(parser.cache.last_history_record.height, 533_000);
        assert_eq!(
            parser.cache.last_ohlc_group.daily.0.t,
            date!(2021 - 07 - 16)
        );
    }

    #[test]
    fn test_generate_ohlc_records_no_events_next_day() {
        let last = OHLCGroup {
            // Tuesday 26 Oct
            daily: DailyOHLC::dummy().date(time::macros::date!(2021 - 10 - 26)),
            // Monday of the week
            weekly: WeeklyOHLC::dummy().date(time::macros::date!(2021 - 10 - 25)),
            // First of the month
            monthly: MonthlyOHLC::dummy().date(time::macros::date!(2021 - 10 - 01)),
        };
        let rc_prices: Vec<NanoERG> = vec![];
        let timestamp = 1635347405599; // WED 2021-10-27;
        let (ds, ws, ms) = generate_ohlc_records(timestamp, &rc_prices, &last);
        assert_eq!(ds.len(), 1);
        assert_eq!(ws.len(), 0);
        assert_eq!(ms.len(), 0);
    }

    #[test]
    fn test_generate_ohlc_records_no_events_next_month() {
        let last = OHLCGroup {
            // Tuesday 26 Oct
            daily: DailyOHLC::dummy().date(time::macros::date!(2021 - 10 - 26)),
            // Monday of the week
            weekly: WeeklyOHLC::dummy().date(time::macros::date!(2021 - 10 - 25)),
            // First of the month
            monthly: MonthlyOHLC::dummy().date(time::macros::date!(2021 - 10 - 01)),
        };
        let rc_prices: Vec<NanoERG> = vec![];
        let timestamp = 1635731294014; // MON 2021-11-01;
        let (ds, ws, ms) = generate_ohlc_records(timestamp, &rc_prices, &last);
        assert_eq!(ds.len(), 6);
        assert_eq!(ws.len(), 1);
        assert_eq!(ms.len(), 1);
    }

    #[test]
    fn test_extract_service_diffs() {
        let service_a: AddressID = AddressID::dummy(123451);
        let service_b: AddressID = AddressID::dummy(123452);
        let events = vec![
            Event::BankTx(BankTransaction {
                index: 1,
                height: 600_000,
                timestamp: 123456789,
                reserves_diff: 1000,
                circ_sc_diff: 100,
                circ_rc_diff: 0,
                box_id: "dummy1".into(),
                service_fee: 2,
                service_address_id: Some(service_a),
            }),
            Event::BankTx(BankTransaction {
                index: 2,
                height: 600_000,
                timestamp: 123456789,
                reserves_diff: 2000,
                circ_sc_diff: 200,
                circ_rc_diff: 0,
                box_id: "dummy2".into(),
                service_fee: 4,
                service_address_id: Some(service_b),
            }),
            Event::BankTx(BankTransaction {
                index: 3,
                height: 600_000,
                timestamp: 123456789,
                reserves_diff: -500,
                circ_sc_diff: 0,
                circ_rc_diff: 100,
                box_id: "dummy3".into(),
                service_fee: 1,
                service_address_id: Some(service_a),
            }),
        ];
        let timestamp = 123456789;
        let service_diffs = extract_service_diffs(timestamp, &events);
        assert_eq!(service_diffs.len(), 3);
        assert_eq!(service_diffs[0].address_id, Some(service_a));

        assert_eq!(service_diffs[0].fees, Decimal::from(2));
        assert_eq!(service_diffs[0].first_tx, timestamp);
        assert_eq!(service_diffs[0].last_tx, timestamp);
        assert_eq!(service_diffs[0].volume, Decimal::from(1000));

        assert_eq!(service_diffs[1].address_id, Some(service_b));
        assert_eq!(service_diffs[1].fees, Decimal::from(4));
        assert_eq!(service_diffs[1].first_tx, timestamp);
        assert_eq!(service_diffs[1].last_tx, timestamp);
        assert_eq!(service_diffs[1].volume, Decimal::from(2000));

        assert_eq!(service_diffs[2].address_id, Some(service_a));
        assert_eq!(service_diffs[2].fees, Decimal::from(1));
        assert_eq!(service_diffs[2].first_tx, timestamp);
        assert_eq!(service_diffs[2].last_tx, timestamp);
        // Volume diffs always positive
        assert_eq!(service_diffs[2].volume, Decimal::from(500));
    }
}

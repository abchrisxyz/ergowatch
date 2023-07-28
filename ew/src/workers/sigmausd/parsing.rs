use std::collections::HashMap;

use super::constants::BANK_NFT;
use super::constants::CONTRACT_ADDRESS_ID;
use super::constants::CONTRACT_CREATION_HEIGHT;
use super::constants::NETWORK_FEE_ADDRESS_ID;
use super::constants::ORACLE_EPOCH_PREP_ADDRESS_ID;
use super::constants::ORACLE_NFT;
use super::constants::RC_TOKEN_ID;
use super::constants::SC_TOKEN_ID;
use super::types::BankTransaction;
use super::types::Batch;
use super::types::DailyOHLC;
use super::types::Event;
use super::types::HistoryRecord;
use super::types::MiniHeader;
use super::types::OraclePosting;
use super::types::ServiceStats;
use crate::core::types::AddressID;
use crate::core::types::Block;
use crate::core::types::CoreData;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Output;
use crate::core::types::Timestamp;
use crate::core::types::Transaction;
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

    pub(super) fn extract_batch(&mut self, data: &CoreData) -> Batch {
        let block = &data.block;
        assert!(block.header.height > CONTRACT_CREATION_HEIGHT);
        let header = MiniHeader::new(
            block.header.height,
            block.header.timestamp,
            block.header.id.clone(),
        );

        // Extract events from block transactions
        let events = extract_events(block, self.cache.bank_transaction_count);

        // Convert events to history records
        let history_records = generate_history_records(&events, &self.cache.last_history_record);

        // Derive a SigRSV for each history records
        let rc_prices: Vec<NanoERG> = history_records.iter().map(|r| r.rc_price()).collect();

        let history_record = history_records.last().map(|hr| hr.clone());

        // OHLC's
        // Could have a new day/week/month without any events, just time passing.
        // Use daily OHLC from current block if any events available.
        // Otherwise use cached one with updated date.
        let block_daily = match DailyOHLC::from_prices(block.header.timestamp, &rc_prices) {
            Some(daily) => daily,
            None => self
                .cache
                .last_ohlc_group
                .daily
                .with_timestamp(block.header.timestamp),
        };
        let block_weekly = block_daily.to_weekly();
        let block_monthly = block_daily.to_monthly();
        let daily_ohlc_records = block_daily.fill_since(&self.cache.last_ohlc_group.daily);
        let weekly_ohlc_records = block_weekly.fill_since(&self.cache.last_ohlc_group.weekly);
        let monthly_ohlc_records = block_monthly.fill_since(&self.cache.last_ohlc_group.monthly);

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
        Batch {
            header,
            events,
            history_record,
            daily_ohlc_records,
            weekly_ohlc_records,
            monthly_ohlc_records,
            service_diffs,
        }
    }
}

fn extract_events(block: &Block, bank_tx_count: i32) -> Vec<Event> {
    let mut local_bank_tx_count = bank_tx_count;
    let height = block.header.height;
    block
        .transactions
        .iter()
        .filter_map(|tx| extract_event(tx, height, &mut local_bank_tx_count))
        .collect()
}

/// Extracts an event from the transaction, if any.
fn extract_event(tx: &Transaction, height: Height, bank_tx_count: &mut i32) -> Option<Event> {
    // Look for presence of bank box in outputs
    if tx_has_bank_box(tx) {
        return Some(Event::BankTx(extract_bank_tx(tx, height, bank_tx_count)));
    } else if tx_has_oracle_prep_box(tx) {
        return Some(Event::Oracle(extract_oracle_posting(tx, height)));
    }
    None
}

fn tx_has_bank_box(tx: &Transaction) -> bool {
    tx.outputs.iter().any(|o| {
        o.address_id == CONTRACT_ADDRESS_ID && o.assets.iter().any(|a| a.token_id == BANK_NFT)
    })
}

fn tx_has_oracle_prep_box(tx: &Transaction) -> bool {
    tx.outputs.iter().any(|o| {
        o.address_id == ORACLE_EPOCH_PREP_ADDRESS_ID
            && o.assets.iter().any(|a| a.token_id == ORACLE_NFT)
    })
}

/// Build a bank transaction from a tx known to contain a bank box
fn extract_bank_tx(tx: &Transaction, height: Height, bank_tx_count: &mut i32) -> BankTransaction {
    // New bank box id
    let bank_outputs: Vec<&Output> = tx
        .outputs
        .iter()
        .filter(|o| {
            o.address_id == CONTRACT_ADDRESS_ID && o.assets.iter().any(|a| a.token_id == BANK_NFT)
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
            .filter(|a| a.token_id == SC_TOKEN_ID)
            .map(|a| a.amount)
            .sum();
        let rc_amount: i64 = output
            .assets
            .iter()
            .filter(|a| a.token_id == RC_TOKEN_ID)
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
            .filter(|a| a.token_id == SC_TOKEN_ID)
            .map(|a| a.amount)
            .sum();
        let rc_amount: i64 = input
            .assets
            .iter()
            .filter(|a| a.token_id == RC_TOKEN_ID)
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

    // Assuming all bank txs change reserves
    assert!(reserves_diff != 0);

    if reserves_diff > 0 {
        // Minting tx
        assert!(circ_sc_diff > 0 || circ_rc_diff > 0);
    } else {
        // Redeeming tx
        assert!(circ_sc_diff < 0 || circ_rc_diff < 0)
    }

    // Assuming any address only in outputs will be a service provider
    let service_candidates: Vec<&AddressID> = erg_diffs
        .keys()
        .filter(|ai| **ai != CONTRACT_ADDRESS_ID && **ai != NETWORK_FEE_ADDRESS_ID)
        .filter(|ai| !sc_diffs.contains_key(ai))
        .filter(|ai| !rc_diffs.contains_key(ai))
        // .map(|ai| *ai)
        .collect();
    let (service_fee, service_address_id) = match service_candidates.len() {
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
            tracing::warn!("multiple service candidates in transaction {}", tx.id);
            (0, None)
        }
    };

    // Now build the event
    *bank_tx_count += 1;
    BankTransaction {
        index: *bank_tx_count,
        height,
        reserves_diff,
        circ_sc_diff,
        circ_rc_diff,
        box_id,
        service_fee,
        service_address_id,
    }
}

/// Build an oracle posting from a tx known to contain an oracle prep box
fn extract_oracle_posting(tx: &Transaction, height: Height) -> OraclePosting {
    // Find the new prep box
    let prep_boxes: Vec<&Output> = tx
        .outputs
        .iter()
        .filter(|o| o.address_id == ORACLE_EPOCH_PREP_ADDRESS_ID)
        .filter(|o| o.assets.iter().any(|a| a.token_id == ORACLE_NFT))
        .collect();
    assert_eq!(prep_boxes.len(), 1);
    let prep_box: &Output = prep_boxes[0];

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
        }
        records.push(new);
        prev = records.last().unwrap();
    }
    records
}

fn extract_service_diffs(timestamp: Timestamp, events: &Vec<Event>) -> Vec<ServiceStats> {
    let mut diffs = vec![];
    for event in events {
        if let Event::BankTx(btx) = event {
            if let Some(aid) = btx.service_address_id {
                diffs.push(ServiceStats {
                    address_id: aid,
                    tx_count: 1,
                    first_tx: timestamp,
                    last_tx: timestamp,
                    fees: btx.service_fee.into(),
                    volume: btx.reserves_diff.into(),
                });
            }
        }
    }
    diffs
}

#[cfg(test)]
mod tests {
    use super::super::types::MonthlyOHLC;
    use super::super::types::WeeklyOHLC;
    use super::*;
    use crate::core::types::Input;
    use crate::core::types::Output;
    use crate::core::types::Transaction;
    use time::macros::date;

    #[test]
    fn test_extract_event_nothing() {
        let input = Input::dummy().address_id(12).value(1000);
        let output0 = Output::dummy().address_id(13).value(900);
        let output1 = Output::dummy().address_id(5).value(100);

        let tx = Transaction::dummy()
            .add_input(input)
            .add_output(output0)
            .add_output(output1);

        let height = 600;
        let mut bank_tx_count = 5;
        let event = extract_event(&tx, height, &mut bank_tx_count);
        assert!(event.is_none());
        assert_eq!(bank_tx_count, 5);
    }

    #[test]
    fn test_extract_event_contract_but_not_bank() {
        let input = Input::dummy().address_id(12).value(1000);
        let output0 = Output::dummy().address_id(CONTRACT_ADDRESS_ID).value(900);
        let output1 = Output::dummy().address_id(5).value(100);

        let tx = Transaction::dummy()
            .add_input(input)
            .add_output(output0)
            .add_output(output1);

        let height = 600;
        let mut bank_tx_count = 5;
        let event = extract_event(&tx, height, &mut bank_tx_count);
        assert!(event.is_none());
        assert_eq!(bank_tx_count, 5);
    }

    #[test]
    fn test_extract_event_sc_mint_direct() {
        // User mints 200 SigUSD for 100 nanoERG
        let user: AddressID = 12345;
        let bank_input = Input::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(SC_TOKEN_ID, 500);
        let bank_output = Output::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(SC_TOKEN_ID, 300);
        let user_input = Input::dummy().address_id(user).value(5000);
        let user_output = Output::dummy()
            .address_id(user)
            .value(4900)
            .add_asset(SC_TOKEN_ID, 200);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output);

        let height = 600;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, &mut new_bank_tx_count).unwrap() {
            Event::BankTx(btx) => {
                assert_eq!(btx.height, height);
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
        let user: AddressID = 12345;
        let service: AddressID = 6789;
        let bank_input = Input::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_TOKEN_ID, 500);
        let bank_output = Output::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_TOKEN_ID, 300);
        let user_input = Input::dummy().address_id(user).value(5103);
        let user_output = Output::dummy()
            .address_id(user)
            .value(5000)
            .add_asset(RC_TOKEN_ID, 200);
        let service_output = Output::dummy().address_id(service).value(2);
        let fee_output = Output::dummy().address_id(NETWORK_FEE_ADDRESS_ID).value(1);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output)
            .add_output(service_output)
            .add_output(fee_output);

        let height = 600;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, &mut new_bank_tx_count).unwrap() {
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
        let user: AddressID = 12345;
        let bank_input = Input::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(SC_TOKEN_ID, 500);
        let bank_output = Output::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(SC_TOKEN_ID, 700);
        let user_input = Input::dummy()
            .address_id(user)
            .value(5005)
            .add_asset(SC_TOKEN_ID, 200);
        let user_output = Output::dummy().address_id(user).value(5100);
        let other1_output = Output::dummy().address_id(30000).value(2);
        let other2_output = Output::dummy().address_id(40000).value(3);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output)
            .add_output(other1_output)
            .add_output(other2_output);

        let height = 600;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, &mut new_bank_tx_count).unwrap() {
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
    fn test_extract_event_rc_redeem_direct() {
        // User redeems 200 SigRSV for 100 nanoERG
        let user: AddressID = 12345;
        let bank_input = Input::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1100)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_TOKEN_ID, 500);
        let bank_output = Output::dummy()
            .address_id(CONTRACT_ADDRESS_ID)
            .value(1000)
            .add_asset(BANK_NFT, 1)
            .add_asset(RC_TOKEN_ID, 700);
        let user_input = Input::dummy()
            .address_id(user)
            .value(5000)
            .add_asset(RC_TOKEN_ID, 200);
        let user_output = Output::dummy().address_id(user).value(5100);

        let tx = Transaction::dummy()
            .add_input(bank_input)
            .add_input(user_input)
            .add_output(bank_output)
            .add_output(user_output);

        let height = 600;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, &mut new_bank_tx_count).unwrap() {
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
    fn test_extract_event_oracle_posting() {
        // Actual prep tx would be spending a live epoch box,
        // but we don't rely on that so can just a dummy.
        let dummy_input = Input::dummy();
        let prep_output = Output::dummy()
            .address_id(ORACLE_EPOCH_PREP_ADDRESS_ID)
            .add_asset(ORACLE_NFT, 1)
            .set_registers(r#"{"R4": "05baafd2a302"}"#);

        let tx = Transaction::dummy()
            .add_input(dummy_input)
            .add_output(prep_output);

        let height = 600;
        let bank_tx_count = 5;
        let mut new_bank_tx_count = bank_tx_count;
        match extract_event(&tx, height, &mut new_bank_tx_count).unwrap() {
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
    pub fn test_history_no_events() {
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
    pub fn test_history_with_bank_event() {
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
    pub fn test_history_with_oracle_event() {
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
    pub fn test_parser_cache_no_events() {
        let cache = ParserCache::dummy();
        let mut parser = Parser::new(cache);
        let data = CoreData {
            block: Block::dummy()
                .height(533_000)
                .timestamp(1626396302125) // 2021-07-16
                .add_tx(
                    Transaction::dummy()
                        .add_input(Input::dummy().value(1000))
                        .add_output(Output::dummy().value(1000)),
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

        let _batch = parser.extract_batch(&data);

        // Check state after
        assert_eq!(parser.cache.bank_transaction_count, 123);
        assert_eq!(parser.cache.last_history_record.height, 1000);
        assert_eq!(
            parser.cache.last_ohlc_group.daily.0.t,
            date!(2021 - 07 - 16)
        );
    }

    #[test]
    pub fn test_parser_cache_with_event() {
        let cache = ParserCache::dummy();
        let mut parser = Parser::new(cache);
        let user: AddressID = 12345;
        let data = CoreData {
            block: Block::dummy()
                .height(533_000)
                .timestamp(1626396302125) // 2021-07-16
                .add_tx(
                    Transaction::dummy()
                        // bank input
                        .add_input(
                            Input::dummy()
                                .address_id(CONTRACT_ADDRESS_ID)
                                .value(1000)
                                .add_asset(BANK_NFT, 1)
                                .add_asset(SC_TOKEN_ID, 500),
                        )
                        // user input
                        .add_input(Input::dummy().address_id(user).value(5000))
                        // bank output
                        .add_output(
                            Output::dummy()
                                .address_id(CONTRACT_ADDRESS_ID)
                                .value(1100)
                                .add_asset(BANK_NFT, 1)
                                .add_asset(SC_TOKEN_ID, 300),
                        )
                        // user output
                        .add_output(
                            Output::dummy()
                                .address_id(user)
                                .value(4900)
                                .add_asset(SC_TOKEN_ID, 200),
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

        let _batch = parser.extract_batch(&data);

        // Check state after
        assert_eq!(parser.cache.bank_transaction_count, 124);
        assert_eq!(parser.cache.last_history_record.height, 533_000);
        assert_eq!(
            parser.cache.last_ohlc_group.daily.0.t,
            date!(2021 - 07 - 16)
        );
    }
}

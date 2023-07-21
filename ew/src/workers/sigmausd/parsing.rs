use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Deref;

use super::constants::BANK_NFT;
use super::constants::CONTRACT_ADDRESS_ID;
use super::constants::CONTRACT_CREATION_HEIGHT;
use super::constants::NETWORK_FEE_ADDRESS_ID;
use super::constants::RC_TOKEN_ID;
use super::constants::SC_TOKEN_ID;
use super::types::BankTransaction;
use super::types::Batch;
use super::types::Event;
use super::types::HistoryRecord;
use super::types::OHLCRecord;
use super::types::OraclePosting;
use super::types::ServiceStats;
use super::types::OHLC;
use crate::core::types::AddressID;
use crate::core::types::Asset;
use crate::core::types::Block;
use crate::core::types::CoreData;
use crate::core::types::Head;
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
    // Last oracle posting
    pub last_oracle_posting: OraclePosting,
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
        let head = Head::new(block.header.height, block.header.id.clone());
        assert!(head.height > CONTRACT_CREATION_HEIGHT);
        let events = extract_events(block, self.cache.bank_transaction_count);
        let history_record = extract_history_record(&events, &self.cache.last_history_record);
        let ohlc_records = extract_ohlc_records(
            block.header.timestamp,
            &history_record,
            &self.cache.last_ohlc_group,
        );
        let service_diffs = extract_service_diffs(&events);
        Batch {
            head,
            events,
            history_record,
            ohlc_records,
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
    }

    // Look for presence of oracle prep box
    None
}

fn tx_has_bank_box(tx: &Transaction) -> bool {
    tx.outputs.iter().any(|o| {
        o.address_id == CONTRACT_ADDRESS_ID && o.assets.iter().any(|a| a.token_id == BANK_NFT)
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

/// Derive new record by applying `events` to `last` one.
fn extract_history_record(events: &Vec<Event>, last: &HistoryRecord) -> Option<HistoryRecord> {
    if events.is_empty() {
        return None;
    }
    let mut hr = last.clone();
    for event in events {
        match event {
            Event::Oracle(oracle_posting) => {
                hr.height = oracle_posting.height;
                hr.oracle = oracle_posting.datapoint;
            }
            Event::BankTx(bank_tx) => {
                hr.height = bank_tx.height;
                hr.circ_sc += bank_tx.circ_sc_diff;
                hr.circ_rc += bank_tx.circ_rc_diff;
                hr.reserves += bank_tx.reserves_diff;
                // Assuming no txs will ever modify both SC and RC.
                // Might be better to use oracle and rsv price instead.
                if bank_tx.circ_sc_diff != 0 {
                    assert_eq!(bank_tx.circ_rc_diff, 0);
                    hr.sc_net += bank_tx.reserves_diff;
                } else if bank_tx.circ_rc_diff != 0 {
                    assert_eq!(bank_tx.circ_sc_diff, 0);
                    hr.rc_net += bank_tx.reserves_diff;
                }
            }
        }
    }
    Some(hr)
}

/// Generate new OHLC records
///
/// t: timestamp of current block
/// hr: history record derived from current block
/// last: last known OHLC
///
/// NEED TO ACCOUNT FOR possible empty periods between last and current history record.
/// This will need to be assessed at each block, not just when there's a sigmausd event.
fn extract_ohlc_records(
    t: Timestamp,
    hr: &Option<HistoryRecord>,
    last: &OHLCGroup,
) -> Vec<OHLCRecord> {
    todo!()
}

fn extract_service_diffs(events: &Vec<Event>) -> Vec<ServiceStats> {
    todo!()
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;
    use crate::core::types::Input;
    use crate::core::types::Output;
    use crate::core::types::Transaction;

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
}

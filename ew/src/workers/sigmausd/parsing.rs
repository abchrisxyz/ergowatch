// use super::super::Parser;
use super::types::BankTransaction;
use super::types::Batch;
use super::types::HistoryRecord;
use super::types::OraclePosting;
use crate::core::types::Block;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Output;
use crate::core::types::Transaction;

pub struct Parser;

impl Parser {
    fn parse_genesis_boxes(&self, outputs: &Vec<Output>) -> Batch {
        todo!()
    }

    fn parse(&mut self, data: &CoreData) -> Batch {
        self.extract_batch(data)
    }
}

impl Parser {
    pub(super) fn extract_batch(&mut self, data: &CoreData) -> Batch {
        let block = &data.block;
        let events = extract_events(block);
        let history_record = extract_history_record(&events);
        let bank_transactions = extract_bank_transactions(block);
        let oracle_posting = extract_oracle_posting(block);
        Batch {
            head: Head::new(block.header.height, block.header.id.clone()),
            bank_transactions,
            oracle_posting,
            history_record,
            ohlc_diff: todo!(),
            service_diffs: todo!(),
        }
    }
}

enum Event {
    /// New oracle price
    Oracle(OraclePosting),
    /// Bank transaction
    BankTx(BankTransaction),
}

fn extract_events(block: &Block) -> Vec<Event> {
    block
        .transactions
        .iter()
        .filter_map(|tx| parse_transaction(tx))
        .collect()
}

/// Extracts an event from the transaction, if any.
fn parse_transaction(tx: &Transaction) -> Option<Event> {
    todo!()
}

fn extract_bank_transactions(block: &Block) -> Vec<BankTransaction> {
    todo!()
}

fn extract_oracle_posting(block: &Block) -> Option<OraclePosting> {
    todo!()
}

fn extract_history_record(events: &Vec<Event>) -> Option<HistoryRecord> {
    todo!()
}

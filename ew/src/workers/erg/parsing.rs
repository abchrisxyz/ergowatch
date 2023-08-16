use std::collections::HashMap;

use super::types::BalanceRecord;
use super::types::Batch;
use super::types::DiffRecord;
use super::types::MiniHeader;
use crate::constants::GENESIS_TIMESTAMP;
use crate::core::types::AddressID;
use crate::core::types::BoxData;
use crate::core::types::CoreData;
use crate::core::types::Head;

mod balances;
mod diffs;

pub struct Parser {
    cache: ParserCache,
}

pub struct ParserCache {}

/// Convenience type to group balance changes together
struct BalanceChanges {
    /// New balance records with new or modified non-zero balances
    pub balance_records: Vec<BalanceRecord>,
    /// Addresses entirely spent in current block
    pub spent_addresses: Vec<AddressID>,
}

impl Parser {
    pub fn new(cache: ParserCache) -> Self {
        Self { cache }
    }

    /// Create a batch from genesis boxes.
    pub fn extract_genesis_batch(&mut self, boxes: &Vec<BoxData>) -> Batch {
        let head = Head::genesis();
        Batch {
            header: MiniHeader {
                height: head.height,
                timestamp: GENESIS_TIMESTAMP,
                id: head.header_id,
            },
            diff_records: boxes
                .iter()
                .map(|b| DiffRecord {
                    address_id: b.address_id,
                    height: 0,
                    tx_idx: 0,
                    nano: b.value,
                })
                .collect(),
            balance_records: boxes
                .iter()
                .map(|b| BalanceRecord::new(b.address_id, b.value, GENESIS_TIMESTAMP))
                .collect(),
            spent_addresses: vec![],
        }
    }

    /// Create a batch from core data.
    pub fn extract_batch(
        &mut self,
        data: &CoreData,
        balances: HashMap<AddressID, BalanceRecord>,
    ) -> Batch {
        let block = &data.block;

        let header = MiniHeader::new(
            block.header.height,
            block.header.timestamp,
            block.header.id.clone(),
        );

        let diff_records = diffs::extract_balance_diffs(&block);
        let balance_changes =
            balances::extract_balance_changes(&balances, &diff_records, block.header.timestamp);

        Batch {
            header,
            balance_records: balance_changes.balance_records,
            spent_addresses: balance_changes.spent_addresses,
            diff_records,
        }
    }
}

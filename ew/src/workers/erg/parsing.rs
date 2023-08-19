use std::collections::HashMap;

use super::types::AddressCounts;
use super::types::BalanceRecord;
use super::types::Batch;
use super::types::Categorized;
use super::types::DiffRecord;
use super::types::MiniHeader;
use crate::constants::GENESIS_TIMESTAMP;
use crate::core::types::AddressID;
use crate::core::types::AddressType;
use crate::core::types::Block;
use crate::core::types::BoxData;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;

mod balances;
mod counts;
mod diffs;

pub struct Parser {
    cache: ParserCache,
}

pub struct ParserCache {
    pub last_address_counts: AddressCounts,
}

/// Holds a diff record with corresponfing address type.
struct TypedDiff {
    pub record: DiffRecord,
    pub address_type: AddressType,
}

impl TypedDiff {
    pub fn new(record: DiffRecord, address_type: AddressType) -> Self {
        Self {
            record,
            address_type,
        }
    }
}

#[derive(Debug, PartialEq)]
struct BalanceChange {
    pub address_id: AddressID,
    pub address_type: AddressType,
    pub old: Option<Balance>,
    pub new: Option<Balance>,
}

/// Balance value and timestamp.
#[derive(Debug, PartialEq)]
struct Balance {
    nano: NanoERG,
    mean_age_timestamp: Timestamp,
}

impl Balance {
    pub fn new(nano: NanoERG, mean_age_timestamp: Timestamp) -> Self {
        Self {
            nano,
            mean_age_timestamp,
        }
    }
}

/// Convenience type to group balance changes together
struct BalanceChanges {
    /// New balance records with new or modified non-zero balances
    pub balance_records: Vec<BalanceRecord>,
    /// Addresses entirely spent in current block
    pub spent_addresses: Vec<AddressID>,
}

impl Categorized<BalanceChanges> {
    pub fn flatten(&self) -> BalanceChanges {
        todo!()
    }
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
            address_counts: todo!(),
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

        let typed_diffs = diffs::extract_balance_diffs(&block);
        let balance_changes =
            balances::extract_balance_changes(&balances, &typed_diffs, block.header.timestamp);

        Batch {
            header,
            address_counts: counts::derive_new_counts(
                &self.cache.last_address_counts,
                &balances,
                &balance_changes,
            ),
            // Extract spent addresses from balance changes
            spent_addresses: balance_changes
                .iter()
                .filter(|bc| bc.new.is_none())
                .map(|bc| bc.address_id)
                .collect(),
            // Extract balance records from balance changes
            balance_records: balance_changes
                .into_iter()
                .filter(|bc| bc.new.is_some())
                .map(|bc| {
                    let bal = bc.new.unwrap();
                    BalanceRecord::new(bc.address_id, bal.nano, bal.mean_age_timestamp)
                })
                .collect(),
            diff_records: typed_diffs.into_iter().map(|td| td.record).collect(),
        }
    }
}

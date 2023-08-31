use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;

use super::types::AddressCounts;
use super::types::BalanceRecord;
use super::types::Batch;
use super::types::CompositionRecord;
use super::types::DiffRecord;
use super::types::MiniHeader;
use crate::constants::GENESIS_TIMESTAMP;
use crate::core::types::AddressID;
use crate::core::types::AddressType;
use crate::core::types::BoxData;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;

mod balances;
mod composition;
mod counts;
mod diffs;

pub struct Parser {
    cache: ParserCache,
}

pub struct ParserCache {
    pub last_address_counts: AddressCounts,
    pub last_supply_composition: CompositionRecord,
}

/// Holds a diff record with corresponfing address type.
struct TypedDiff {
    pub record: DiffRecord,
    pub address_type: AddressType,
}

#[cfg(test)]
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

pub(super) enum Bal {
    Spent,
    Unspent(Balance),
}

impl Bal {
    pub fn update(&self, amount: NanoERG, timestamp: Timestamp) -> Self {
        match self {
            // No existing balance so diff becomes new balance
            Bal::Spent => Bal::Unspent(Balance::new(amount, timestamp)),
            // Update existing balance
            Bal::Unspent(balance) => {
                let new_nano = balance.nano + amount;
                if new_nano == 0 {
                    // Balance got spent entirely
                    return Bal::Spent;
                }
                let new_mat = if amount > 0 {
                    // Credit refreshes balance age
                    ((Decimal::from_i64(balance.mean_age_timestamp).unwrap()
                        * Decimal::from_i64(balance.nano).unwrap()
                        + Decimal::from_i64(timestamp).unwrap()
                            * Decimal::from_i64(amount).unwrap())
                        / Decimal::from_i64(new_nano).unwrap())
                    .to_i64()
                    .unwrap()
                } else {
                    // Partial spend does not change balance age
                    balance.mean_age_timestamp
                };
                Bal::Unspent(Balance::new(new_nano, new_mat))
            }
        }
    }
}

/// Balance value and timestamp.
#[derive(Debug, PartialEq)]
pub(super) struct Balance {
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

    pub fn zero() -> Self {
        Self {
            nano: 0,
            mean_age_timestamp: 0,
        }
    }

    pub fn update(&self, amount: NanoERG, timestamp: Timestamp) -> Self {
        match self.nano == 0 {
            // No existing balance so diff becomes new balance
            True => Balance::new(amount, timestamp),
            // Update existing balance
            False => {
                let new_nano = self.nano + amount;
                if new_nano == 0 {
                    // Balance got spent entirely
                    return Balance::zero();
                }
                let new_mat = if amount > 0 {
                    // Credit refreshes balance age
                    ((Decimal::from_i64(self.mean_age_timestamp).unwrap()
                        * Decimal::from_i64(self.nano).unwrap()
                        + Decimal::from_i64(timestamp).unwrap()
                            * Decimal::from_i64(amount).unwrap())
                        / Decimal::from_i64(new_nano).unwrap())
                    .to_i64()
                    .unwrap()
                } else {
                    // Partial spend does not change balance age
                    self.mean_age_timestamp
                };
                Balance::new(new_nano, new_mat)
            }
        }
    }
}

impl Parser {
    pub fn new(cache: ParserCache) -> Self {
        Self { cache }
    }

    /// Create a batch from genesis boxes.
    pub fn extract_genesis_batch(&mut self, boxes: &Vec<BoxData>) -> Batch {
        let head = Head::genesis();

        let balance_changes: Vec<BalanceChange> = boxes
            .iter()
            .map(|bx| BalanceChange {
                address_id: bx.address_id,
                address_type: bx.address_type.clone(),
                old: None,
                new: Some(Balance::new(bx.value, GENESIS_TIMESTAMP)),
            })
            .collect();

        self.cache.last_supply_composition = composition::from_genesis_boxes(&boxes);

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
            address_counts: counts::derive_new_counts(
                &self.cache.last_address_counts,
                &balance_changes,
            ),
            supply_composition: self.cache.last_supply_composition.clone(),
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
                &balance_changes,
            ),
            supply_composition: composition::derive_record(
                &self.cache.last_supply_composition,
                &typed_diffs,
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

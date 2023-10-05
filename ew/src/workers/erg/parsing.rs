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

/// Holds a diff record with corresponding address type.
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
    pub old: Bal,
    pub new: Bal,
}

#[derive(Debug, PartialEq)]
pub(super) enum Bal {
    Spent,
    Unspent(Balance),
}

impl From<&BalanceRecord> for Bal {
    fn from(br: &BalanceRecord) -> Self {
        Bal::Unspent(Balance::new(br.nano, br.mean_age_timestamp))
    }
}

impl From<Option<&BalanceRecord>> for Bal {
    fn from(value: Option<&BalanceRecord>) -> Self {
        match value {
            None => Self::Spent,
            Some(br) => Bal::Unspent(Balance::new(br.nano, br.mean_age_timestamp)),
        }
    }
}

impl Bal {
    // /// Returns true if balance is spent entirely
    // pub fn is_spent(&self) -> bool {
    //     matches!(self, Self::Spent)
    // }

    /// Returns true if balance is non zero
    pub fn is_unspent(&self) -> bool {
        matches!(self, Self::Unspent(_))
    }

    /// Return new `Bal` with accrued value and timestamp.
    pub fn accrue(&self, amount: NanoERG, timestamp: Timestamp) -> Self {
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

    /// Reverse accrual of given diff `amount` applied at `timestamp`.
    /// 
    /// * `amount`: the diff amount previously accrued and to be reversed.
    /// * `timestamp`: timestamp of the block the diff was accrued.
    pub fn reverse(&self, amount: NanoERG, timestamp: Timestamp) -> Self {
        match self {
            Bal::Spent => panic!("Can't reverse value of a spent balance. Instead, restore it from earlier balance diff records, if any."),
            Bal::Unspent(bal) => {
                if amount == 0 {
                    return Self::Unspent(Balance::new(amount, timestamp));
                }
                if amount == bal.nano {
                    return Self::Spent;
                }
                let reversed_nano = bal.nano - amount;
                
                let reversed_mat = if amount <= 0 {
                    // Balance was decreased, so timestamp unaffected.
                    // Restore by adding nano's back.
                    bal.mean_age_timestamp
                } else {
                // Balance was added to, so timestamp increased.
                    assert!(reversed_nano > 0);
                    ((Decimal::from_i64(bal.mean_age_timestamp).unwrap()
                        * Decimal::from_i64(bal.nano).unwrap()
                        - Decimal::from_i64(timestamp).unwrap()
                            * Decimal::from_i64(amount).unwrap())
                        / Decimal::from_i64(reversed_nano).unwrap())
                    .to_i64()
                    .unwrap()
                };
                Self::Unspent(Balance::new(reversed_nano, reversed_mat))
            }
        }
    }
}

/// Balance value and timestamp.
#[derive(Debug, PartialEq)]
pub(super) struct Balance {
    pub nano: NanoERG,
    pub mean_age_timestamp: Timestamp,
}

impl Balance {
    pub fn new(nano: NanoERG, mean_age_timestamp: Timestamp) -> Self {
        Self {
            nano,
            mean_age_timestamp,
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
                old: Bal::Spent,
                new: Bal::Unspent(Balance::new(bx.value, GENESIS_TIMESTAMP)),
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
                .filter(|bc| matches!(bc.new, Bal::Spent))
                .map(|bc| bc.address_id)
                .collect(),
            // Extract balance records from balance changes
            balance_records: balance_changes
                .into_iter()
                .filter_map(|bc| match bc.new {
                    Bal::Spent => None,
                    Bal::Unspent(bal) => Some(BalanceRecord::new(
                        bc.address_id,
                        bal.nano,
                        bal.mean_age_timestamp,
                    )),
                })
                .collect(),
            diff_records: typed_diffs.into_iter().map(|td| td.record).collect(),
        }
    }
}


#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;

    const TS_10K: Timestamp = 1563159993440; // timestamp of block 10000

    #[test]
    fn test_bal_roundtrip_positive_diff() {
        let bal = Bal::Unspent(Balance::new(1000, TS_10K));
        let diff_amount = 300;
        let diff_ts = TS_10K + 120_000;
        let roundtripped_bal = bal.accrue(diff_amount, diff_ts).reverse(diff_amount, diff_ts);
        match roundtripped_bal {
            Bal::Spent => panic!(),
            Bal::Unspent(b) => {
                assert_eq!(1000, b.nano);
                // Allow 1ms rounding error
                assert_eq!((TS_10K - b.mean_age_timestamp).abs(), 1);
            }
        }
    }

    #[test]
    fn test_bal_roundtrip_negative_diff() {
        let bal = Bal::Unspent(Balance::new(1000, TS_10K));
        let diff_amount = -500;
        let diff_ts = TS_10K + 120_000;
        assert_eq!(bal, bal.accrue(diff_amount, diff_ts).reverse(diff_amount, diff_ts));
    }
}

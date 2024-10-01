use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;

use super::types::AddressCounts;
use super::types::BalanceRecord;
use super::types::Batch;
use super::types::CompositionRecord;
use crate::core::types::AddressID;
use crate::core::types::AddressType;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;
use crate::framework::StampedData;
use crate::workers::erg_diffs::types::DiffData;

mod balances;
mod composition;
mod counts;

pub struct Parser {
    cache: ParserCache,
}

pub struct ParserCache {
    pub last_address_counts: AddressCounts,
    pub last_supply_composition: CompositionRecord,
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

    /// Create a batch from core data.
    pub fn extract_batch(
        &mut self,
        stamped_data: &StampedData<DiffData>,
        balances: HashMap<AddressID, BalanceRecord>,
    ) -> StampedData<Batch> {
        let diffs = &stamped_data.data.diff_records;
        let balance_changes =
            balances::extract_balance_changes(&balances, diffs, stamped_data.timestamp);

        self.cache.last_address_counts =
            counts::derive_new_counts(&self.cache.last_address_counts, &balance_changes);
        self.cache.last_supply_composition =
            composition::derive_record(&self.cache.last_supply_composition, diffs);

        stamped_data.wrap(Batch {
            // Extract spent addresses from balance changes
            spent_addresses: balance_changes
                .iter()
                .filter(|bc| matches!(bc.new, Bal::Spent))
                .map(|bc| bc.address_id)
                .collect(),
            // Extract new addresses from balance changes
            new_addresses: balance_changes
                .iter()
                .filter(|bc| matches!(bc.old, Bal::Spent))
                .filter(|bc| matches!(bc.new, Bal::Unspent(_)))
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
            address_counts: self.cache.last_address_counts.clone(),
            supply_composition: self.cache.last_supply_composition.clone(),
        })
    }
}

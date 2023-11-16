use postgres_types::ToSql;

use crate::core::types::AddressID;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use std::collections::HashSet;

pub struct Batch {
    pub diff_records: Vec<DiffRecord>,
}

#[cfg_attr(feature = "test-utilities", derive(Clone))]
/// Downstream data produced by `erg_diffs` worker.
pub struct DiffData {
    pub diff_records: Vec<DiffRecord>,
}

impl DiffData {
    /// Get all address id's present in diff records.
    pub fn diffed_addresses(&self) -> Vec<AddressID> {
        HashSet::<AddressID>::from_iter(self.diff_records.iter().map(|r| r.address_id))
            .into_iter()
            .collect()
    }
}

impl From<Batch> for DiffData {
    fn from(batch: Batch) -> Self {
        Self {
            diff_records: batch.diff_records,
        }
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "test-utilities", derive(Clone))]
pub struct DiffRecord {
    pub address_id: AddressID,
    pub height: Height,
    pub tx_idx: i16,
    pub nano: NanoERG,
}

impl DiffRecord {
    pub fn new(address_id: AddressID, height: Height, tx_idx: i16, nano: NanoERG) -> Self {
        Self {
            address_id,
            height,
            tx_idx,
            nano,
        }
    }
}

/// An address agnostic balance change.
#[derive(Debug, ToSql)]
pub struct SupplyDiff {
    pub height: Height,
    pub nano: NanoERG,
}

impl SupplyDiff {
    pub fn new(height: Height, nano: NanoERG) -> Self {
        Self { height, nano }
    }
}

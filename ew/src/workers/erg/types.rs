use crate::core::types::AddressID;
use crate::core::types::Head;
use crate::core::types::HeaderID;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;

pub struct Batch {
    pub header: MiniHeader,
    pub diff_records: Vec<DiffRecord>,
    pub balance_records: Vec<BalanceRecord>,
    /// Address id's who's balance became zero
    pub spent_addresses: Vec<AddressID>,
    /// Address counts
    pub address_counts: AddressCounts,
}

pub struct MiniHeader {
    pub height: Height,
    pub timestamp: Timestamp,
    pub id: HeaderID,
}

impl MiniHeader {
    pub fn new(height: Height, timestamp: Timestamp, id: HeaderID) -> Self {
        Self {
            height,
            timestamp,
            id,
        }
    }

    pub fn head(&self) -> Head {
        Head::new(self.height, self.id.clone())
    }
}

/// Holds data for each address type
pub struct Categorized<T> {
    pub p2pk: T,
    pub contracts: T,
    pub miners: T,
}

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

#[derive(Debug, Clone, PartialEq)]
pub struct BalanceRecord {
    pub address_id: AddressID,
    pub nano: NanoERG,
    pub mean_age_timestamp: Timestamp,
}

impl BalanceRecord {
    pub fn new(address_id: AddressID, nano: NanoERG, mean_age_timestamp: Timestamp) -> Self {
        Self {
            address_id,
            nano,
            mean_age_timestamp,
        }
    }
}

pub struct AddressCounts {
    pub p2pk: AddressCountsRecord,
    pub contracts: AddressCountsRecord,
    pub miners: AddressCountsRecord,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AddressCountsRecord {
    pub height: Height,
    pub total: i64,
    pub ge_0p001: i64,
    pub ge_0p01: i64,
    pub ge_0p1: i64,
    pub ge_1: i64,
    pub ge_10: i64,
    pub ge_100: i64,
    pub ge_1k: i64,
    pub ge_10k: i64,
    pub ge_100k: i64,
    pub ge_1m: i64,
}

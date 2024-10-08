use postgres_from_row::FromRow;

use crate::core::types::AddressID;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;

pub struct Batch {
    pub balance_records: Vec<BalanceRecord>,
    /// Address id's whose balance became zero
    pub spent_addresses: Vec<AddressID>,
    /// Address id's whose balance was zero and is isn't anymore
    pub new_addresses: Vec<AddressID>,
    /// Address counts
    pub address_counts: AddressCounts,
    /// Supply composition (supply on different address types)
    pub supply_composition: CompositionRecord,
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

#[derive(Debug, Clone)]
pub struct AddressCounts {
    pub p2pk: AddressCountsRecord,
    pub contracts: AddressCountsRecord,
    pub miners: AddressCountsRecord,
}

#[derive(Debug, Clone, PartialEq, FromRow)]
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

impl AddressCountsRecord {
    pub fn blank() -> Self {
        Self {
            height: -1,
            total: 0,
            ge_0p001: 0,
            ge_0p01: 0,
            ge_0p1: 0,
            ge_1: 0,
            ge_10: 0,
            ge_100: 0,
            ge_1k: 0,
            ge_10k: 0,
            ge_100k: 0,
            ge_1m: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompositionRecord {
    pub height: Height,
    // Supply on *all* P2PK addresses
    pub p2pks: NanoERG,
    // Supply on non-mining contracts, excluding (re-emission)
    pub contracts: NanoERG,
    // Supply on mining contracts
    pub miners: NanoERG,
}

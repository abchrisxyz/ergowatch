use crate::core::types::Address;
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

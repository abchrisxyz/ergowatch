use crate::core::types::AddressID;
use crate::core::types::AssetID;
use crate::core::types::Height;
use crate::core::types::Value;

pub struct Batch {
    pub diff_records: Vec<DiffRecord>,
    /// Balance records to be upserted
    pub balance_records: Vec<BalanceRecord>,
    /// Address id's who's asset id balance became zero (to be deleted)
    pub spent_addresses: Vec<AddressAsset>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct AddressAsset(pub AddressID, pub AssetID);

impl AddressAsset {
    pub fn new(address_id: AddressID, asset_id: AssetID) -> Self {
        Self(address_id, asset_id)
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "test-utilities", derive(Clone))]
pub struct DiffRecord {
    pub address_id: AddressID,
    pub asset_id: AssetID,
    pub height: Height,
    pub tx_idx: i16,
    pub value: Value,
}

impl DiffRecord {
    pub fn new(
        address_id: AddressID,
        asset_id: AssetID,
        height: Height,
        tx_idx: i16,
        value: Value,
    ) -> Self {
        Self {
            address_id,
            asset_id,
            height,
            tx_idx,
            value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BalanceRecord {
    pub address_id: AddressID,
    pub asset_id: AssetID,
    pub value: Value,
}

impl BalanceRecord {
    pub fn new(address_id: AddressID, asset_id: AssetID, value: Value) -> Self {
        Self {
            address_id,
            asset_id,
            value,
        }
    }
}

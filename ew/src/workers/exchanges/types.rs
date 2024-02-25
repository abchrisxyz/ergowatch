use postgres_from_row::FromRow;

use crate::core::types::AddressID;
use crate::core::types::Height;
use crate::core::types::NanoERG;
pub(super) use crate::workers::erg_diffs::types::SupplyDiff;

pub type ExchangeID = i32;

pub struct Batch {
    /// Main and deposit supply across all exchanges - not including `supply_patch`
    pub supply: SupplyRecord,
    /// A timeseries of supply changes to be applied to deposit supply records
    /// to reflect new deposit addresses and conflicts.
    /// Applies to latest `supply` record too!
    pub supply_patch: Vec<SupplyDiff>,
    /// Any new deposit addresses spotted in current block
    pub deposit_addresses: Vec<DepositAddressRecord>,
    /// Any conflicts spotted in current block
    pub deposit_conflicts: Vec<DepositAddressConflict>,
}

// pub struct ExchangeRecord {
//     id: ExchangeID,
//     text_id: String,
//     name: String,
//     listing_height: Height,
// }

#[derive(Debug, Clone, PartialEq)]
pub struct SupplyRecord {
    pub height: Height,
    pub main: NanoERG,
    pub deposits: NanoERG,
}

#[derive(Debug, FromRow)]
pub struct DepositAddressRecord {
    pub address_id: AddressID,
    pub cex_id: ExchangeID,
    pub spot_height: Height,
}

pub struct DepositAddressConflict {
    pub address_id: AddressID,
    pub first_cex_id: Option<ExchangeID>,
    pub conflict_spot_height: Height,
}

impl DepositAddressConflict {
    pub fn to_record(&self, deposit_spot_height: Height) -> DepositAddressConflictRecord {
        DepositAddressConflictRecord {
            address_id: self.address_id,
            first_cex_id: self.first_cex_id,
            deposit_spot_height,
            conflict_spot_height: self.conflict_spot_height,
        }
    }
}

#[derive(Debug)]
pub struct DepositAddressConflictRecord {
    pub address_id: AddressID,
    pub first_cex_id: Option<ExchangeID>,
    pub deposit_spot_height: Height,
    pub conflict_spot_height: Height,
}

impl From<DepositAddressConflictRecord> for DepositAddressRecord {
    fn from(value: DepositAddressConflictRecord) -> Self {
        Self {
            address_id: value.address_id,
            cex_id: value.first_cex_id.expect("an exchange id"),
            spot_height: value.deposit_spot_height,
        }
    }
}

#[derive(Debug)]
pub struct ExchangeRecord {
    pub id: ExchangeID,
    pub text_id: String,
    pub name: String,
}

#[derive(Debug)]
pub struct MainAddressRecord {
    pub address_id: AddressID,
    pub cex_id: ExchangeID,
    pub address: String,
}

impl MainAddressRecord {
    pub fn new(address_id: AddressID, cex_id: ExchangeID, address: &str) -> Self {
        Self {
            address_id,
            cex_id,
            address: address.to_owned(),
        }
    }
}

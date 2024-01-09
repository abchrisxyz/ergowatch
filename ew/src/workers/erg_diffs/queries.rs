use super::types::SupplyDiff;
use crate::core::types::AddressID;
use crate::core::types::Height;

/// A balance diffs series query.
///
/// Yields balance diffs for all given address ids.
#[derive(Debug)]
pub struct DiffsQuery {
    pub(super) address_ids: Vec<AddressID>,
    pub(super) max_height: Height,
}

impl DiffsQuery {
    pub fn new(address_ids: Vec<AddressID>, max_height: Height) -> Self {
        Self {
            address_ids,
            max_height,
        }
    }
}

pub type DiffsQueryResponse = Vec<SupplyDiff>;

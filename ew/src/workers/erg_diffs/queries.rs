use super::types::SupplyDiff;
use crate::core::types::AddressID;

/// A balance diffs series query.
///
/// Yields balance diffs for all given address ids.
#[derive(Debug)]
pub struct DiffsQuery {
    pub(super) address_ids: Vec<AddressID>,
}

impl DiffsQuery {
    pub fn new(address_ids: Vec<AddressID>) -> Self {
        Self { address_ids }
    }
}

pub type DiffsQueryResponse = Vec<SupplyDiff>;

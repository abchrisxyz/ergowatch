mod balances;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;

use super::types::AddressAsset;
use super::types::BalanceRecord;
use super::types::Batch;
use super::types::DiffRecord;
use crate::core::types::AddressID;
use crate::core::types::AssetID;
use crate::core::types::Block;
use crate::core::types::CoreData;
use crate::core::types::Height;
use crate::core::types::Transaction;
use crate::core::types::Value;
use crate::framework::StampedData;

#[derive(Debug, PartialEq)]
struct BalanceChange {
    pub address_id: AddressID,
    pub asset_id: AssetID,
    pub old: Bal,
    pub new: Bal,
}

#[derive(Debug, PartialEq)]
pub(super) enum Bal {
    Spent,
    Unspent(Value),
}

impl From<&BalanceRecord> for Bal {
    fn from(br: &BalanceRecord) -> Self {
        Bal::Unspent(br.value)
    }
}

impl From<Option<&BalanceRecord>> for Bal {
    fn from(value: Option<&BalanceRecord>) -> Self {
        match value {
            None => Self::Spent,
            Some(br) => Bal::Unspent(br.value),
        }
    }
}

impl Bal {
    /// Returns true if balance is non zero
    pub fn is_unspent(&self) -> bool {
        matches!(self, Self::Unspent(_))
    }

    /// Return new `Bal` with accrued value.
    pub fn accrue(&self, amount: Value) -> Self {
        match self {
            // No existing balance so diff becomes new balance
            Bal::Spent => Bal::Unspent(amount),
            // Update existing balance
            Bal::Unspent(current_value) => {
                let new_value = current_value + amount;
                if new_value == 0 {
                    // Balance got spent entirely
                    Bal::Spent
                } else {
                    Bal::Unspent(new_value)
                }
            }
        }
    }
}

pub struct Parser {}

impl Parser {
    pub fn new() -> Self {
        Self {}
    }

    pub fn extract_diffs(&self, stamped_data: &StampedData<CoreData>) -> Vec<DiffRecord> {
        extract_diff_records(&stamped_data.data.block)
    }

    pub fn extract_batch(
        &self,
        stamped_data: &StampedData<CoreData>,
        diff_records: Vec<DiffRecord>,
        balances: HashMap<(AddressID, AssetID), BalanceRecord>,
    ) -> StampedData<Batch> {
        let balance_changes = balances::extract_balance_changes(&balances, &diff_records);
        let batch = Batch {
            diff_records,
            // Extract balance records from balance changes
            balance_records: balance_changes
                .into_iter()
                .filter_map(|bc| match bc.new {
                    Bal::Spent => None,
                    Bal::Unspent(value) => {
                        Some(BalanceRecord::new(bc.address_id, bc.asset_id, value))
                    }
                })
                .collect(),
            spent_addresses: vec![],
        };
        stamped_data.wrap(batch)
    }
}

/// Get all (AddressID, AssetID) pairs presen in diff records.
pub fn diffed_address_assets(diff_records: &Vec<DiffRecord>) -> Vec<AddressAsset> {
    HashSet::<AddressAsset>::from_iter(
        diff_records
            .iter()
            .map(|r| AddressAsset::new(r.address_id, r.asset_id)),
    )
    .into_iter()
    .collect()
}

/// Extract non-zero balance diffs from transactions
pub(super) fn extract_diff_records(block: &Block) -> Vec<DiffRecord> {
    block
        .transactions
        .iter()
        .enumerate()
        .flat_map(|(idx, tx)| parse_tx(tx, block.header.height, idx as i16))
        .collect()
}

/// Generates a collection of diff records from a block transaction.
fn parse_tx(tx: &Transaction, height: Height, tx_idx: i16) -> Vec<DiffRecord> {
    let mut map: HashMap<(AddressID, AssetID), Value> = HashMap::new();
    for input in &tx.inputs {
        for asset in &input.assets {
            match map.entry((input.address_id, asset.asset_id)) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() -= asset.amount;
                }
                Entry::Vacant(e) => {
                    e.insert(-asset.amount);
                }
            }
        }
    }
    for output in &tx.outputs {
        for asset in &output.assets {
            match map.entry((output.address_id, asset.asset_id)) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() += asset.amount;
                }
                Entry::Vacant(e) => {
                    e.insert(asset.amount);
                }
            }
        }
    }
    map.into_iter()
        .filter(|(_, value)| value != &0)
        .map(|((address_id, asset_id), value)| DiffRecord {
            address_id,
            asset_id,
            height,
            tx_idx,
            value,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::Block;
    use crate::core::types::BoxData;
    use crate::core::types::Transaction;

    #[test]
    fn test_simple_transfer() {
        let addr_a = AddressID(123);
        let addr_b = AddressID(456);
        let asset_x: AssetID = 9;
        let block = Block::dummy().height(123456).add_tx(
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(addr_a).add_asset(asset_x, 1000))
                .add_output(BoxData::dummy().address_id(addr_b).add_asset(asset_x, 1000)),
        );
        let mut recs = extract_diff_records(&block);
        recs.sort_by_key(|e| (e.tx_idx, e.address_id.0, e.asset_id));
        assert_eq!(recs.len(), 2);
        // 1st tx - address A got - 1000
        assert_eq!(recs[0].address_id, addr_a);
        assert_eq!(recs[0].height, 123456);
        assert_eq!(recs[0].tx_idx, 0);
        assert_eq!(recs[0].value, -1000);
        // 1st tx - address B got + 1000
        assert_eq!(recs[1].address_id, addr_b);
        assert_eq!(recs[1].height, 123456);
        assert_eq!(recs[1].tx_idx, 0);
        assert_eq!(recs[1].value, 1000);
    }

    #[test]
    fn test_multiple_transfers() {
        // A sends 400 to B
        // C does nothing (might have been a token transfer)
        // B consolidates
        let addr_a = AddressID(123);
        let addr_b = AddressID(456);
        let addr_c = AddressID(789);
        let asset_x: AssetID = 8;
        let asset_y: AssetID = 9;
        let block = Block::dummy()
            .height(123456)
            .add_tx(
                Transaction::dummy()
                    .add_input(BoxData::dummy().address_id(addr_a).add_asset(asset_x, 1000))
                    .add_input(BoxData::dummy().address_id(addr_b).add_asset(asset_x, 2000))
                    .add_input(BoxData::dummy().address_id(addr_b).add_asset(asset_x, 1000))
                    .add_input(BoxData::dummy().address_id(addr_c).add_asset(asset_x, 5000))
                    .add_output(BoxData::dummy().address_id(addr_a).add_asset(asset_x, 600))
                    .add_output(BoxData::dummy().address_id(addr_b).add_asset(asset_x, 3400))
                    .add_output(BoxData::dummy().address_id(addr_c).add_asset(asset_x, 5000))
                    .add_output(BoxData::dummy().address_id(addr_c).add_asset(asset_y, 5000)),
            )
            .add_tx(
                // C sends 300 to A
                Transaction::dummy()
                    .add_input(BoxData::dummy().address_id(addr_c).add_asset(asset_x, 5000))
                    .add_output(BoxData::dummy().address_id(addr_a).add_asset(asset_x, 300))
                    .add_output(BoxData::dummy().address_id(addr_c).add_asset(asset_x, 4700)),
            );
        let mut recs = extract_diff_records(&block);
        recs.sort_by_key(|e| (e.tx_idx, e.address_id.0, e.asset_id));
        assert_eq!(recs.len(), 5);
        // 1st tx - address A got - 400 x
        assert_eq!(recs[0].address_id, addr_a);
        assert_eq!(recs[0].asset_id, asset_x);
        assert_eq!(recs[0].height, 123456);
        assert_eq!(recs[0].tx_idx, 0);
        assert_eq!(recs[0].value, -400);
        // 1st tx - address B got + 400 x
        assert_eq!(recs[1].address_id, addr_b);
        assert_eq!(recs[1].asset_id, asset_x);
        assert_eq!(recs[1].height, 123456);
        assert_eq!(recs[1].tx_idx, 0);
        assert_eq!(recs[1].value, 400);
        // 1st tx - address C got + 5000 y
        assert_eq!(recs[2].address_id, addr_c);
        assert_eq!(recs[2].asset_id, asset_y);
        assert_eq!(recs[2].height, 123456);
        assert_eq!(recs[2].tx_idx, 0);
        assert_eq!(recs[2].value, 5000);
        // 2nd tx - address A got + 300 x
        assert_eq!(recs[3].address_id, addr_a);
        assert_eq!(recs[3].asset_id, asset_x);
        assert_eq!(recs[3].height, 123456);
        assert_eq!(recs[3].tx_idx, 1);
        assert_eq!(recs[3].value, 300);
        // 2nd tx - address C got - 300 x
        assert_eq!(recs[4].address_id, addr_c);
        assert_eq!(recs[4].asset_id, asset_x);
        assert_eq!(recs[4].height, 123456);
        assert_eq!(recs[4].tx_idx, 1);
        assert_eq!(recs[4].value, -300);
    }
}

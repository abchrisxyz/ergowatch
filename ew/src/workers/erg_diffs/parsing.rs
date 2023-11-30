use std::collections::hash_map::Entry;
use std::collections::HashMap;

use itertools::Itertools;

use super::types::Batch;
use super::types::DiffRecord;
use crate::core::types::AddressID;
use crate::core::types::Block;
use crate::core::types::CoreData;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Transaction;
use crate::framework::StampedData;

pub struct Parser {}

impl Parser {
    pub fn new() -> Self {
        Self {}
    }

    pub fn extract_batch(&self, stamped_data: &StampedData<CoreData>) -> StampedData<Batch> {
        let batch = Batch {
            diff_records: extract_diff_records(&stamped_data.data.block),
        };
        stamped_data.wrap(batch)
    }
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
    let mut map: HashMap<AddressID, NanoERG> = HashMap::new();
    for input in &tx.inputs {
        match map.entry(input.address_id) {
            Entry::Occupied(mut e) => {
                *e.get_mut() -= input.value;
            }
            Entry::Vacant(e) => {
                e.insert(-input.value);
            }
        }
    }
    for output in &tx.outputs {
        match map.entry(output.address_id) {
            Entry::Occupied(mut e) => {
                *e.get_mut() += output.value;
            }
            Entry::Vacant(e) => {
                e.insert(output.value);
            }
        }
    }
    map.into_iter()
        .filter(|(_, nano)| nano != &0)
        .map(|(address_id, nano)| DiffRecord {
            address_id: address_id,
            height,
            tx_idx,
            nano,
        })
        // Order by amount - for consistency across instances and tests
        .sorted_by_key(|r| r.nano)
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
        let block = Block::dummy().height(123456).add_tx(
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(addr_a).value(1000))
                .add_output(BoxData::dummy().address_id(addr_b).value(1000)),
        );
        let recs = extract_diff_records(&block);
        assert_eq!(recs.len(), 2);
        // 1st tx - address A got - 1000
        assert_eq!(recs[0].address_id, addr_a);
        assert_eq!(recs[0].height, 123456);
        assert_eq!(recs[0].tx_idx, 0);
        assert_eq!(recs[0].nano, -1000);
        // 1st tx - address B got + 1000
        assert_eq!(recs[1].address_id, addr_b);
        assert_eq!(recs[1].height, 123456);
        assert_eq!(recs[1].tx_idx, 0);
        assert_eq!(recs[1].nano, 1000);
    }

    #[test]
    fn test_multiple_transfers() {
        // A sends 400 to B
        // C does nothing (might have been a token transfer)
        // B consolidates
        let addr_a = AddressID(123);
        let addr_b = AddressID(456);
        let addr_c = AddressID(789);
        let block = Block::dummy()
            .height(123456)
            .add_tx(
                Transaction::dummy()
                    .add_input(BoxData::dummy().address_id(addr_a).value(1000))
                    .add_input(BoxData::dummy().address_id(addr_b).value(2000))
                    .add_input(BoxData::dummy().address_id(addr_b).value(1000))
                    .add_input(BoxData::dummy().address_id(addr_c).value(5000))
                    .add_output(BoxData::dummy().address_id(addr_a).value(600))
                    .add_output(BoxData::dummy().address_id(addr_b).value(3400))
                    .add_output(BoxData::dummy().address_id(addr_c).value(5000)),
            )
            .add_tx(
                // C sends 300 to A
                Transaction::dummy()
                    .add_input(BoxData::dummy().address_id(addr_c).value(5000))
                    .add_output(BoxData::dummy().address_id(addr_a).value(300))
                    .add_output(BoxData::dummy().address_id(addr_c).value(4700)),
            );
        let recs = extract_diff_records(&block);
        assert_eq!(recs.len(), 4);
        // 1st tx - address A got - 400
        assert_eq!(recs[0].address_id, addr_a);
        assert_eq!(recs[0].height, 123456);
        assert_eq!(recs[0].tx_idx, 0);
        assert_eq!(recs[0].nano, -400);
        // 1st tx - address B got + 400
        assert_eq!(recs[1].address_id, addr_b);
        assert_eq!(recs[1].height, 123456);
        assert_eq!(recs[1].tx_idx, 0);
        assert_eq!(recs[1].nano, 400);
        // 2nd tx - address C got - 300
        assert_eq!(recs[2].address_id, addr_c);
        assert_eq!(recs[2].height, 123456);
        assert_eq!(recs[2].tx_idx, 1);
        assert_eq!(recs[2].nano, -300);
        // 2nd tx - address A got + 300
        assert_eq!(recs[3].address_id, addr_a);
        assert_eq!(recs[3].height, 123456);
        assert_eq!(recs[3].tx_idx, 1);
        assert_eq!(recs[3].nano, 300);
    }
}

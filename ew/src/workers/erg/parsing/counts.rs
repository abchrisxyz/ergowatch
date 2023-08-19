use std::collections::HashMap;

use super::super::types::AddressCountsRecord;
use super::AddressCounts;
use super::BalanceChange;
use super::BalanceChanges;
use crate::core::types::AddressID;

use crate::workers::erg::types::BalanceRecord;

pub(super) fn derive_new_counts(
    cache: &AddressCounts,
    start_balances: &HashMap<AddressID, BalanceRecord>,
    balance_changes: &Vec<BalanceChange>,
) -> AddressCounts {
    todo!()
    // AddressCounts {
    //     p2pk: step(&cache.p2pk, &start_balances, &balance_changes.p2pk),
    //     contracts: step(
    //         &cache.contracts,
    //         &start_balances,
    //         &balance_changes.contracts,
    //     ),
    //     miners: step(&cache.miners, &start_balances, &balance_changes.miners),
    // }
}

fn step(
    prev: &AddressCountsRecord,
    start_balances: &HashMap<AddressID, BalanceRecord>,
    balance_changes: &BalanceChanges,
) -> AddressCountsRecord {
    let mut counts = prev.clone();
    // Spent addresses
    for address_id in &balance_changes.spent_addresses {
        let spent_bal = start_balances[&address_id].nano;
        // if spent_bal...
    }
    counts
}

struct Counter {
    /// Counts, from total to ge_1m
    counts: [i64; 11],
}

impl Counter {
    /// Build a `Counter` from an `AddressCountsRecord`
    pub fn new(rec: &AddressCountsRecord) -> Self {
        Self {
            counts: [
                rec.total,
                rec.ge_0p001,
                rec.ge_0p01,
                rec.ge_0p1,
                rec.ge_1,
                rec.ge_10,
                rec.ge_100,
                rec.ge_1k,
                rec.ge_10k,
                rec.ge_100k,
                rec.ge_1m,
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_changes() {
        let rec = AddressCountsRecord {
            height: 100_000,
            total: 10_000,
            ge_0p001: 9000,
            ge_0p01: 8000,
            ge_0p1: 7000,
            ge_1: 6000,
            ge_10: 5000,
            ge_100: 4000,
            ge_1k: 3000,
            ge_10k: 2000,
            ge_100k: 1000,
            ge_1m: 0,
        };
        let start_balances: HashMap<AddressID, BalanceRecord> = HashMap::from([
            (123, BalanceRecord::new(123, 1000, 0)),
            (456, BalanceRecord::new(456, 2000, 0)),
        ]);
        let balance_changes = BalanceChanges {
            balance_records: vec![],
            spent_addresses: vec![],
        };
        let res = step(&rec, &start_balances, &balance_changes);
        let expected = AddressCountsRecord {
            height: 100_000,
            total: 10_000,
            ge_0p001: 9000,
            ge_0p01: 8000,
            ge_0p1: 7000,
            ge_1: 6000,
            ge_10: 5000,
            ge_100: 4000,
            ge_1k: 3000,
            ge_10k: 2000,
            ge_100k: 1000,
            ge_1m: 0,
        };
        assert_eq!(rec, res);
    }
}

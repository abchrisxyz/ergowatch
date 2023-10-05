use std::collections::hash_map::Entry;
use std::collections::HashMap;

use super::super::types::BalanceRecord;
use super::Bal;

use super::BalanceChange;
use super::TypedDiff;
use crate::core::types::AddressID;
use crate::core::types::Timestamp;

/// Returns balance changes, ignoring addresses created and spent in same block.
///
/// * `balances`: previous, non-zero, balance records for addresses
///    present in `diff_records`
/// * `typed_diffs`: collections of tx-level non-zero balance diffs
/// * `timestamp`: timestamp of current block
pub(super) fn extract_balance_changes(
    balances: &HashMap<AddressID, BalanceRecord>,
    typed_diffs: &Vec<TypedDiff>,
    timestamp: Timestamp,
) -> Vec<BalanceChange> {
    // Rewrite this using Parsing::Bal

    let mut balance_changes: HashMap<AddressID, BalanceChange> = HashMap::new();
    // Apply diffs to balances
    for diff in typed_diffs {
        let address_id = diff.record.address_id;
        match balance_changes.entry(diff.record.address_id) {
            // No existing balance change
            Entry::Vacant(entry) => {
                // Check for existing balance record and convert to balance
                let old_bal = Bal::from(balances.get(&address_id));
                // Insert new BalanceChange
                entry.insert(BalanceChange {
                    address_id: address_id,
                    address_type: diff.address_type.clone(),
                    new: old_bal.accrue(diff.record.nano, timestamp),
                    old: old_bal,
                });
            }
            // We've seen this address before, apply diff
            Entry::Occupied(mut entry) => {
                let mut bc = entry.get_mut();
                bc.new = bc.new.accrue(diff.record.nano, timestamp)
            }
        }
    }
    balance_changes
        .into_values()
        // Drop addresses created and spent in same block
        .filter(|bc| bc.old.is_unspent() || bc.new.is_unspent())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::AddressType::P2PK;
    use super::super::Balance;
    use super::super::DiffRecord;
    use super::*;
    use crate::core::types::NanoERG;
    use rust_decimal::prelude::FromPrimitive;
    use rust_decimal::prelude::ToPrimitive;
    use rust_decimal::Decimal;

    const ADDR_A: AddressID = 123;
    const ADDR_B: AddressID = 456;
    const ADDR_C: AddressID = 789;
    const TS_10K: Timestamp = 1563159993440; // timestamp of block 10000
    const TS_20K: Timestamp = 1564413706977; // timestamp of block 20000
    const TS_30K: Timestamp = 1565532307779; // timestamp of block 30000

    #[test]
    fn test_full_transfer() {
        let balances: HashMap<AddressID, BalanceRecord> =
            HashMap::from([(ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K))]);
        let typed_diffs = vec![
            TypedDiff::new(DiffRecord::new(ADDR_A, 30000, 0, -2000), P2PK),
            TypedDiff::new(DiffRecord::new(ADDR_B, 30000, 0, 2000), P2PK),
        ];
        let changes = extract_balance_changes(&balances, &typed_diffs, TS_30K);
        assert_eq!(changes.len(), 2);
        // A got spent entirely
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(2000, TS_10K)),
            new: Bal::Spent
        }));
        // B got a fresh new balance with timestamp of current block
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            address_type: P2PK,
            old: Bal::Spent,
            new: Bal::Unspent(Balance::new(2000, TS_30K)),
        }));
    }

    #[test]
    fn test_partial_spend() {
        let balances: HashMap<AddressID, BalanceRecord> =
            HashMap::from([(ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K))]);
        let typed_diffs = vec![
            TypedDiff::new(DiffRecord::new(ADDR_A, 30000, 0, -500), P2PK),
            TypedDiff::new(DiffRecord::new(ADDR_B, 30000, 0, 500), P2PK),
        ];
        let changes = extract_balance_changes(&balances, &typed_diffs, TS_30K);
        assert_eq!(changes.len(), 2);
        println!("{:?}", changes[0]);
        println!("{:?}", changes[1]);
        // A got a lower balance with unchanged timestamp
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(2000, TS_10K)),
            new: Bal::Unspent(Balance::new(1500, TS_10K)),
        }));
        // B got a fresh new balance with timestamp of current block
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            address_type: P2PK,
            old: Bal::Spent,
            new: Bal::Unspent(Balance::new(500, TS_30K)),
        }));
    }

    #[test]
    fn test_partial_credit() {
        let balances: HashMap<AddressID, BalanceRecord> = HashMap::from([
            (ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K)),
            (ADDR_B, BalanceRecord::new(ADDR_B, 1500, TS_20K)),
        ]);
        let typed_diffs = vec![
            TypedDiff::new(DiffRecord::new(ADDR_A, 30000, 0, -500), P2PK),
            TypedDiff::new(DiffRecord::new(ADDR_B, 30000, 0, 500), P2PK),
        ];
        let changes = extract_balance_changes(&balances, &typed_diffs, TS_30K);
        assert_eq!(changes.len(), 2);
        // A got a lower balance with unchanged timestamp
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(2000, TS_10K)),
            new: Bal::Unspent(Balance::new(1500, TS_10K)),
        }));
        // B got a higher balance with more recent timestamp
        let ts_b = Decimal::from_f32(0.75).unwrap() * Decimal::from_i64(TS_20K).unwrap()
            + Decimal::from_f32(0.25).unwrap() * Decimal::from_i64(TS_30K).unwrap();
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(1500, TS_20K)),
            new: Bal::Unspent(Balance::new(2000, ts_b.to_i64().unwrap())),
        }));
    }

    #[test]
    fn test_partial_credit_fractional() {
        // Variant with fractional age calculation
        let balances: HashMap<AddressID, BalanceRecord> = HashMap::from([
            (ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K)),
            (ADDR_B, BalanceRecord::new(ADDR_B, 1000, TS_20K)),
        ]);
        let typed_diffs = vec![
            TypedDiff::new(DiffRecord::new(ADDR_A, 30000, 0, -2000), P2PK),
            TypedDiff::new(DiffRecord::new(ADDR_B, 30000, 0, 2000), P2PK),
        ];
        let changes = extract_balance_changes(&balances, &typed_diffs, TS_30K);
        assert_eq!(changes.len(), 2);
        // A got spent
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(2000, TS_10K)),
            new: Bal::Spent,
        }));
        // B got a higher balance with more recent timestamp
        let ts_b = (Decimal::from_i64(1).unwrap() * Decimal::from_i64(TS_20K).unwrap()
            + Decimal::from_i64(2).unwrap() * Decimal::from_i64(TS_30K).unwrap())
            / Decimal::from_i64(3).unwrap();
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(1000, TS_20K)),
            new: Bal::Unspent(Balance::new(3000, ts_b.to_i64().unwrap())),
        }));
    }

    #[test]
    fn test_spend_then_credit() {
        // Case where an address gets spent then credited again in same block.
        let balances: HashMap<AddressID, BalanceRecord> = HashMap::from([
            (ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K)),
            (ADDR_C, BalanceRecord::new(ADDR_C, 3000, TS_20K)),
        ]);
        let typed_diffs = vec![
            // Send A to B, spending A entirely
            TypedDiff::new(DiffRecord::new(ADDR_A, 30000, 0, -2000), P2PK),
            TypedDiff::new(DiffRecord::new(ADDR_B, 30000, 0, 2000), P2PK),
            // Then send C to A
            TypedDiff::new(DiffRecord::new(ADDR_C, 30000, 1, -3000), P2PK),
            TypedDiff::new(DiffRecord::new(ADDR_A, 30000, 1, 3000), P2PK),
        ];
        let changes = extract_balance_changes(&balances, &typed_diffs, TS_30K);
        assert_eq!(changes.len(), 3);
        // A got a new balance
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(2000, TS_10K)),
            new: Bal::Unspent(Balance::new(3000, TS_30K)),
        }));
        // B got a new balance
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            address_type: P2PK,
            old: Bal::Spent,
            new: Bal::Unspent(Balance::new(2000, TS_30K)),
        }));
        // C got spent entirely
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_C,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(3000, TS_20K)),
            new: Bal::Spent,
        }));
    }

    #[test]
    fn test_credit_then_spend() {
        // Ensure an address created and spent in same block appears nowhere
        let balances: HashMap<AddressID, BalanceRecord> =
            HashMap::from([(ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K))]);
        let typed_diffs = vec![
            // Send A to B
            TypedDiff::new(DiffRecord::new(ADDR_A, 30000, 0, -2000), P2PK),
            TypedDiff::new(DiffRecord::new(ADDR_B, 30000, 0, 2000), P2PK),
            // Then spend B back to A
            TypedDiff::new(DiffRecord::new(ADDR_B, 30000, 1, -2000), P2PK),
            TypedDiff::new(DiffRecord::new(ADDR_A, 30000, 1, 2000), P2PK),
        ];
        let changes = extract_balance_changes(&balances, &typed_diffs, TS_30K);
        // Check there are no changes for address B
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].address_id, ADDR_A);
    }

    #[test]
    fn test_big_numbers() {
        // Makes sure age calculation doesn't hit integer overflows
        let million: NanoERG = 1_000_000_000_000_000;
        let balances: HashMap<AddressID, BalanceRecord> = HashMap::from([
            (ADDR_A, BalanceRecord::new(ADDR_A, 20 * million, TS_10K)),
            (ADDR_B, BalanceRecord::new(ADDR_B, 3 * million, TS_20K)),
        ]);
        let typed_diffs = vec![
            TypedDiff::new(DiffRecord::new(ADDR_A, 30000, 0, -1 * million), P2PK),
            TypedDiff::new(DiffRecord::new(ADDR_B, 30000, 0, 1 * million), P2PK),
        ];
        let changes = extract_balance_changes(&balances, &typed_diffs, TS_30K);
        assert_eq!(changes.len(), 2);
        // A got a lower balance with unchanged timestamp
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(20 * million, TS_10K)),
            new: Bal::Unspent(Balance::new(19 * million, TS_10K)),
        }));
        // B got a higher balance with more recent timestamp
        let ts_b = Decimal::from_f32(0.75).unwrap() * Decimal::from_i64(TS_20K).unwrap()
            + Decimal::from_f32(0.25).unwrap() * Decimal::from_i64(TS_30K).unwrap();
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            address_type: P2PK,
            old: Bal::Unspent(Balance::new(3 * million, TS_20K)),
            new: Bal::Unspent(Balance::new(4 * million, ts_b.to_i64().unwrap())),
        }));
    }
}

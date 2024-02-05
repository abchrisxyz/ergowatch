use std::collections::hash_map::Entry;
use std::collections::HashMap;

use super::super::types::BalanceRecord;
use super::Bal;

use super::super::types::DiffRecord;
use super::BalanceChange;
use crate::core::types::AddressID;
use crate::core::types::AssetID;

/// Returns balance changes, ignoring addresses created and spent in same block.
///
/// * `balances`: previous, non-zero, balance records for addresses
///    present in `diff_records`
/// * `typed_diffs`: collections of tx-level non-zero balance diffs
/// * `timestamp`: timestamp of current block
pub(super) fn extract_balance_changes(
    balances: &HashMap<(AddressID, AssetID), BalanceRecord>,
    diffs: &Vec<DiffRecord>,
) -> Vec<BalanceChange> {
    // Rewrite this using Parsing::Bal

    let mut balance_changes: HashMap<(AddressID, AssetID), BalanceChange> = HashMap::new();
    // Apply diffs to balances
    for diff in diffs {
        // let address_id = diff.address_id;
        match balance_changes.entry((diff.address_id, diff.asset_id)) {
            // No existing balance change
            Entry::Vacant(entry) => {
                // Check for existing balance record and convert to balance
                let old_bal = Bal::from(balances.get(&(diff.address_id, diff.asset_id)));
                // Insert new BalanceChange
                entry.insert(BalanceChange {
                    address_id: diff.address_id,
                    asset_id: diff.asset_id,
                    new: old_bal.accrue(diff.value),
                    old: old_bal,
                });
            }
            // We've seen this address before, apply diff
            Entry::Occupied(mut entry) => {
                let bc = entry.get_mut();
                bc.new = bc.new.accrue(diff.value);
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
    use super::*;
    use crate::core::types::Value;

    const ADDR_A: AddressID = AddressID(123_1);
    const ADDR_B: AddressID = AddressID(456_1);
    const ADDR_C: AddressID = AddressID(789_1);
    const ASSET_X: AssetID = 11;

    #[test]
    fn test_full_transfer() {
        let balances: HashMap<(AddressID, AssetID), BalanceRecord> =
            HashMap::from([((ADDR_A, ASSET_X), BalanceRecord::new(ADDR_A, ASSET_X, 2000))]);
        let diffs = vec![
            DiffRecord::new(ADDR_A, ASSET_X, 30000, 0, -2000),
            DiffRecord::new(ADDR_B, ASSET_X, 30000, 0, 2000),
        ];
        let changes = extract_balance_changes(&balances, &diffs);
        assert_eq!(changes.len(), 2);
        // A got spent entirely
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            asset_id: ASSET_X,
            old: Bal::Unspent(2000),
            new: Bal::Spent
        }));
        // B got a fresh new balance with timestamp of current block
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            asset_id: ASSET_X,
            old: Bal::Spent,
            new: Bal::Unspent(2000),
        }));
    }

    #[test]
    fn test_partial_spend() {
        let balances: HashMap<(AddressID, AssetID), BalanceRecord> =
            HashMap::from([((ADDR_A, ASSET_X), BalanceRecord::new(ADDR_A, ASSET_X, 2000))]);
        let diffs = vec![
            DiffRecord::new(ADDR_A, ASSET_X, 30000, 0, -500),
            DiffRecord::new(ADDR_B, ASSET_X, 30000, 0, 500),
        ];
        let changes = extract_balance_changes(&balances, &diffs);
        assert_eq!(changes.len(), 2);
        println!("{:?}", changes[0]);
        println!("{:?}", changes[1]);
        // A got a lower balance with unchanged timestamp
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            asset_id: ASSET_X,
            old: Bal::Unspent(2000),
            new: Bal::Unspent(1500),
        }));
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            asset_id: ASSET_X,
            old: Bal::Spent,
            new: Bal::Unspent(500),
        }));
    }

    #[test]
    fn test_partial_credit() {
        let balances: HashMap<(AddressID, AssetID), BalanceRecord> = HashMap::from([
            ((ADDR_A, ASSET_X), BalanceRecord::new(ADDR_A, ASSET_X, 2000)),
            ((ADDR_B, ASSET_X), BalanceRecord::new(ADDR_B, ASSET_X, 1500)),
        ]);
        let diffs = vec![
            DiffRecord::new(ADDR_A, ASSET_X, 30000, 0, -500),
            DiffRecord::new(ADDR_B, ASSET_X, 30000, 0, 500),
        ];
        let changes = extract_balance_changes(&balances, &diffs);
        assert_eq!(changes.len(), 2);
        // A got a lower balance with unchanged timestamp
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            asset_id: ASSET_X,
            old: Bal::Unspent(2000),
            new: Bal::Unspent(1500),
        }));
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            asset_id: ASSET_X,
            old: Bal::Unspent(1500),
            new: Bal::Unspent(2000),
        }));
    }

    #[test]
    fn test_partial_credit_fractional() {
        // Variant with fractional age calculation
        let balances: HashMap<(AddressID, AssetID), BalanceRecord> = HashMap::from([
            ((ADDR_A, ASSET_X), BalanceRecord::new(ADDR_A, ASSET_X, 2000)),
            ((ADDR_B, ASSET_X), BalanceRecord::new(ADDR_B, ASSET_X, 1000)),
        ]);
        let diffs = vec![
            DiffRecord::new(ADDR_A, ASSET_X, 30000, 0, -2000),
            DiffRecord::new(ADDR_B, ASSET_X, 30000, 0, 2000),
        ];
        let changes = extract_balance_changes(&balances, &diffs);
        assert_eq!(changes.len(), 2);
        // A got spent
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            asset_id: ASSET_X,
            old: Bal::Unspent(2000),
            new: Bal::Spent,
        }));
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            asset_id: ASSET_X,
            old: Bal::Unspent(1000),
            new: Bal::Unspent(3000),
        }));
    }

    #[test]
    fn test_spend_then_credit() {
        // Case where an address gets spent then credited again in same block.
        let balances: HashMap<(AddressID, AssetID), BalanceRecord> = HashMap::from([
            ((ADDR_A, ASSET_X), BalanceRecord::new(ADDR_A, ASSET_X, 2000)),
            ((ADDR_C, ASSET_X), BalanceRecord::new(ADDR_C, ASSET_X, 3000)),
        ]);
        let diffs = vec![
            // Send A to B, spending A entirely
            DiffRecord::new(ADDR_A, ASSET_X, 30000, 0, -2000),
            DiffRecord::new(ADDR_B, ASSET_X, 30000, 0, 2000),
            // Then send C to A
            DiffRecord::new(ADDR_C, ASSET_X, 30000, 1, -3000),
            DiffRecord::new(ADDR_A, ASSET_X, 30000, 1, 3000),
        ];
        let changes = extract_balance_changes(&balances, &diffs);
        assert_eq!(changes.len(), 3);
        // A got a new balance
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            asset_id: ASSET_X,
            old: Bal::Unspent(2000),
            new: Bal::Unspent(3000),
        }));
        // B got a new balance
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            asset_id: ASSET_X,
            old: Bal::Spent,
            new: Bal::Unspent(2000),
        }));
        // C got spent entirely
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_C,
            asset_id: ASSET_X,
            old: Bal::Unspent(3000),
            new: Bal::Spent,
        }));
    }

    #[test]
    fn test_credit_then_spend() {
        // Ensure an address created and spent in same block appears nowhere
        let balances: HashMap<(AddressID, AssetID), BalanceRecord> =
            HashMap::from([((ADDR_A, ASSET_X), BalanceRecord::new(ADDR_A, ASSET_X, 2000))]);
        let diffs = vec![
            // Send A to B
            DiffRecord::new(ADDR_A, ASSET_X, 30000, 0, -2000),
            DiffRecord::new(ADDR_B, ASSET_X, 30000, 0, 2000),
            // Then spend B back to A
            DiffRecord::new(ADDR_B, ASSET_X, 30000, 1, -2000),
            DiffRecord::new(ADDR_A, ASSET_X, 30000, 1, 2000),
        ];
        let changes = extract_balance_changes(&balances, &diffs);
        // Check there are no changes for address B
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].address_id, ADDR_A);
    }

    #[test]
    fn test_big_numbers() {
        // Makes sure age calculation doesn't hit integer overflows
        let million: Value = 1_000_000_000_000_000;
        let balances: HashMap<(AddressID, AssetID), BalanceRecord> = HashMap::from([
            (
                (ADDR_A, ASSET_X),
                BalanceRecord::new(ADDR_A, ASSET_X, 20 * million),
            ),
            (
                (ADDR_B, ASSET_X),
                BalanceRecord::new(ADDR_B, ASSET_X, 3 * million),
            ),
        ]);
        let diffs = vec![
            DiffRecord::new(ADDR_A, ASSET_X, 30000, 0, -1 * million),
            DiffRecord::new(ADDR_B, ASSET_X, 30000, 0, 1 * million),
        ];
        let changes = extract_balance_changes(&balances, &diffs);
        assert_eq!(changes.len(), 2);
        // A got a lower balance with unchanged timestamp
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_A,
            asset_id: ASSET_X,
            old: Bal::Unspent(20 * million),
            new: Bal::Unspent(19 * million),
        }));
        assert!(changes.contains(&BalanceChange {
            address_id: ADDR_B,
            asset_id: ASSET_X,
            old: Bal::Unspent(3 * million),
            new: Bal::Unspent(4 * million),
        }));
    }
}

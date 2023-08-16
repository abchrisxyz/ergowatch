use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;

use super::super::types::BalanceRecord;
use super::super::types::DiffRecord;
use super::BalanceChanges;
use crate::core::types::AddressID;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;

/// Returns balance records for modified (but still non-zero) balances
/// as well as a collection of entirely spent addresses.
///
/// * `balances`: previous, non-zero, balance records for addresses
///    present in `diff_records`
/// * `diff_records`: collection of tx-level non-zero balance diffs
/// * `timestamp`: timestamp of current block
pub(super) fn extract_balance_changes(
    balances: &HashMap<AddressID, BalanceRecord>,
    diff_records: &Vec<DiffRecord>,
    timestamp: Timestamp,
) -> BalanceChanges {
    let mut balances: HashMap<AddressID, BalanceRecord> = balances.clone();
    let mut spent_addresses: HashSet<AddressID> = HashSet::new();
    // Apply diffs to existing balances
    for diff in diff_records {
        match balances.entry(diff.address_id) {
            // No existing balance
            Entry::Vacant(entry) => {
                // Addresses's entire balance created just now, so use current timestamp
                entry.insert(BalanceRecord::new(diff.address_id, diff.nano, timestamp));
                // Address could have been spent in a previous tx, so remove from spent
                // addresses to be sure.
                spent_addresses.remove(&diff.address_id);
            }
            // Existing balance
            Entry::Occupied(entry) => {
                let old_balance: NanoERG = entry.get().nano;
                let new_balance = old_balance + diff.nano;
                if new_balance == 0 {
                    // Spent entirely, remove from balances altogether.
                    let spent = entry.remove_entry();
                    // And keep as spent address
                    spent_addresses.insert(spent.0);
                } else if new_balance < old_balance {
                    // Partial spend does not change balance age
                    entry.into_mut().nano = new_balance;
                } else if new_balance > old_balance {
                    // Credit refreshes balance age
                    let me = entry.into_mut();
                    me.nano = new_balance;
                    me.mean_age_timestamp = ((Decimal::from_i64(me.mean_age_timestamp).unwrap()
                        * Decimal::from_i64(old_balance).unwrap()
                        + Decimal::from_i64(timestamp).unwrap()
                            * Decimal::from_i64(diff.nano).unwrap())
                        / Decimal::from_i64(new_balance).unwrap())
                    .to_i64()
                    .unwrap();
                } else {
                    panic!("Unhandled case processing balance changes")
                }
            }
        }
    }
    // Collect into vector and filter out zero balances
    BalanceChanges {
        balance_records: balances.into_values().collect(),
        spent_addresses: spent_addresses.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADDR_A: AddressID = 123;
    const ADDR_B: AddressID = 456;
    const ADDR_C: AddressID = 789;
    const TS_10K: Timestamp = 1563159993440; // timestamp of block 30000
    const TS_20K: Timestamp = 1564413706977; // timestamp of block 30000
    const TS_30K: Timestamp = 1565532307779; // timestamp of block 30000

    #[test]
    fn test_full_transfer() {
        let balances: HashMap<AddressID, BalanceRecord> =
            HashMap::from([(ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K))]);
        let diff_records = vec![
            DiffRecord::new(ADDR_A, 30000, 0, -2000),
            DiffRecord::new(ADDR_B, 30000, 0, 2000),
        ];
        let changes = extract_balance_changes(&balances, &diff_records, TS_30K);
        let records = changes.balance_records;
        // A got spent entirely
        assert_eq!(changes.spent_addresses.len(), 1);
        assert!(changes.spent_addresses.contains(&ADDR_A));
        // Just one changed addresses
        assert_eq!(records.len(), 1);
        // B got a fresh new balance with timestamp of current block
        assert!(records.contains(&BalanceRecord::new(ADDR_B, 2000, TS_30K)));
    }

    #[test]
    fn test_partial_spend() {
        let balances: HashMap<AddressID, BalanceRecord> =
            HashMap::from([(ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K))]);
        let diff_records = vec![
            DiffRecord::new(ADDR_A, 30000, 0, -500),
            DiffRecord::new(ADDR_B, 30000, 0, 500),
        ];
        let changes = extract_balance_changes(&balances, &diff_records, TS_30K);
        let records = changes.balance_records;
        // No spent addresses
        assert_eq!(changes.spent_addresses.len(), 0);
        // Two changed addresses
        assert_eq!(records.len(), 2);
        // A got a lower balance with unchanged timestamp
        assert!(records.contains(&BalanceRecord::new(ADDR_A, 1500, TS_10K)));
        // B got a fresh new balance with timestamp of current block
        assert!(records.contains(&BalanceRecord::new(ADDR_B, 500, TS_30K)));
    }

    #[test]
    fn test_partial_credit() {
        let balances: HashMap<AddressID, BalanceRecord> = HashMap::from([
            (ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K)),
            (ADDR_B, BalanceRecord::new(ADDR_B, 1500, TS_20K)),
        ]);
        let diff_records = vec![
            DiffRecord::new(ADDR_A, 30000, 0, -500),
            DiffRecord::new(ADDR_B, 30000, 0, 500),
        ];
        let changes = extract_balance_changes(&balances, &diff_records, TS_30K);
        let records = changes.balance_records;
        // No spent addresses
        assert_eq!(changes.spent_addresses.len(), 0);
        // Two changed addresses
        assert_eq!(records.len(), 2);
        // A got a lower balance with unchanged timestamp
        assert!(records.contains(&BalanceRecord::new(ADDR_A, 1500, TS_10K)));
        // B got a higher balance with more recent timestamp
        let ts_b = Decimal::from_f32(0.75).unwrap() * Decimal::from_i64(TS_20K).unwrap()
            + Decimal::from_f32(0.25).unwrap() * Decimal::from_i64(TS_30K).unwrap();
        assert!(records.contains(&BalanceRecord::new(ADDR_B, 2000, ts_b.to_i64().unwrap())));
    }

    #[test]
    fn test_partial_credit_fractional() {
        // Variant with fractional age calculation
        let balances: HashMap<AddressID, BalanceRecord> = HashMap::from([
            (ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K)),
            (ADDR_B, BalanceRecord::new(ADDR_B, 1000, TS_20K)),
        ]);
        let diff_records = vec![
            DiffRecord::new(ADDR_A, 30000, 0, -2000),
            DiffRecord::new(ADDR_B, 30000, 0, 2000),
        ];
        let changes = extract_balance_changes(&balances, &diff_records, TS_30K);
        let records = changes.balance_records;
        // 1 spent addresses
        assert_eq!(changes.spent_addresses.len(), 1);
        assert!(changes.spent_addresses.contains(&ADDR_A));
        // 1 changed addresses
        assert_eq!(records.len(), 1);
        // B got a higher balance with more recent timestamp
        let ts_b = (Decimal::from_i64(1).unwrap() * Decimal::from_i64(TS_20K).unwrap()
            + Decimal::from_i64(2).unwrap() * Decimal::from_i64(TS_30K).unwrap())
            / Decimal::from_i64(3).unwrap();
        assert_eq!(records[0].mean_age_timestamp, ts_b.to_i64().unwrap());
        assert!(records.contains(&BalanceRecord::new(ADDR_B, 3000, ts_b.to_i64().unwrap())));
    }

    #[test]
    fn test_spend_then_credit() {
        // Ensure an address being spent then credited again in same block
        // is not flagged as spent.
        let balances: HashMap<AddressID, BalanceRecord> = HashMap::from([
            (ADDR_A, BalanceRecord::new(ADDR_A, 2000, TS_10K)),
            (ADDR_C, BalanceRecord::new(ADDR_C, 3000, TS_20K)),
        ]);
        let diff_records = vec![
            // Send A to B, spending A entirely
            DiffRecord::new(ADDR_A, 30000, 0, -2000),
            DiffRecord::new(ADDR_B, 30000, 0, 2000),
            // Then send C to A
            DiffRecord::new(ADDR_C, 30000, 1, -3000),
            DiffRecord::new(ADDR_A, 30000, 1, 3000),
        ];
        let changes = extract_balance_changes(&balances, &diff_records, TS_30K);
        let records = changes.balance_records;
        // 1 spent addresses
        assert_eq!(changes.spent_addresses.len(), 1);
        assert!(changes.spent_addresses.contains(&ADDR_C));
        // 2 changed addresses
        assert_eq!(records.len(), 2);
        // A got a new balance
        assert!(records.contains(&BalanceRecord::new(ADDR_A, 3000, TS_30K)));
        // B got a new balance
        assert!(records.contains(&BalanceRecord::new(ADDR_B, 2000, TS_30K)));
    }

    #[test]
    fn test_big_numbers() {
        // Makes sure age calculation doesn't hit integer overflows
        let million: NanoERG = 1_000_000_000_000_000;
        let balances: HashMap<AddressID, BalanceRecord> = HashMap::from([
            (ADDR_A, BalanceRecord::new(ADDR_A, 20 * million, TS_10K)),
            (ADDR_B, BalanceRecord::new(ADDR_B, 3 * million, TS_20K)),
        ]);
        let diff_records = vec![
            DiffRecord::new(ADDR_A, 30000, 0, -1 * million),
            DiffRecord::new(ADDR_B, 30000, 0, 1 * million),
        ];
        let changes = extract_balance_changes(&balances, &diff_records, TS_30K);
        let records = changes.balance_records;
        // No spent addresses
        assert_eq!(changes.spent_addresses.len(), 0);
        // Two changed addresses
        assert_eq!(records.len(), 2);
        // A got a lower balance with unchanged timestamp
        assert!(records.contains(&BalanceRecord::new(ADDR_A, 19 * million, TS_10K)));
        // B got a higher balance with more recent timestamp
        let ts_b = Decimal::from_f32(0.75).unwrap() * Decimal::from_i64(TS_20K).unwrap()
            + Decimal::from_f32(0.25).unwrap() * Decimal::from_i64(TS_30K).unwrap();
        assert!(records.contains(&BalanceRecord::new(
            ADDR_B,
            4 * million,
            ts_b.to_i64().unwrap()
        )));
    }
}

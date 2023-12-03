//! Address counts by balance and type
use super::super::types::AddressCountsRecord;
use super::AddressCounts;
use super::Bal;
use super::BalanceChange;

use crate::core::types::AddressType;
use crate::core::types::Height;
use crate::core::types::NanoERG;

pub(super) fn derive_new_counts(
    cache: &AddressCounts,
    balance_changes: &Vec<BalanceChange>,
) -> AddressCounts {
    AddressCounts {
        p2pk: count(&cache.p2pk, &balance_changes, AddressType::P2PK),
        contracts: count(&cache.contracts, &balance_changes, AddressType::Other),
        miners: count(&cache.miners, &balance_changes, AddressType::Miner),
    }
}

/// Return new address counts resulting from applying balance changes
/// of given `address_type` to existing address counts.
/// Height of new address counts is incremented by 1.
///
/// * `prev`: latest address counts
/// * `balance_changes`: collection of balance changes to be applied
/// * `address_type`: type of addresses to consider - all others are ignored
fn count(
    prev: &AddressCountsRecord,
    balance_changes: &Vec<BalanceChange>,
    address_type: AddressType,
) -> AddressCountsRecord {
    let mut counter = Counter::new(prev);

    for change in balance_changes
        .iter()
        .filter(|bc| bc.address_type == address_type)
    {
        counter.apply(change);
    }

    counter.to_record(prev.height + 1)
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

    pub fn to_record(&self, height: Height) -> AddressCountsRecord {
        AddressCountsRecord {
            height: height,
            total: self.counts[0],
            ge_0p001: self.counts[1],
            ge_0p01: self.counts[2],
            ge_0p1: self.counts[3],
            ge_1: self.counts[4],
            ge_10: self.counts[5],
            ge_100: self.counts[6],
            ge_1k: self.counts[7],
            ge_10k: self.counts[8],
            ge_100k: self.counts[9],
            ge_1m: self.counts[10],
        }
    }

    /// Register balance change across impacted counts.
    pub fn apply(&mut self, change: &BalanceChange) {
        let iold = match &change.old {
            Bal::Spent => 0,
            Bal::Unspent(bal) => index(bal.nano) + 1,
        };
        let inew = match &change.new {
            Bal::Spent => 0,
            Bal::Unspent(bal) => index(bal.nano) + 1,
        };
        if iold < inew {
            self.counts[iold..inew].iter_mut().for_each(|e| *e += 1);
        } else if iold > inew {
            self.counts[inew..iold].iter_mut().for_each(|e| *e -= 1);
        }
    }
}

/// Returns index of last bin `nano` amount fits in.
///
/// E.g.    1 ERG is >= 1, returns 4
/// E.g. 3000 ERG is >= 1k returns 7
fn index(nano: NanoERG) -> usize {
    if nano >= 1_000_000_000_000_000 {
        // 1M ERG
        return 10;
    } else if nano >= 100_000_000_000_000 {
        // 100k ERG
        return 9;
    } else if nano >= 10_000_000_000_000 {
        // 10k ERG
        return 8;
    } else if nano >= 1_000_000_000_000 {
        // 1000 ERG
        return 7;
    } else if nano >= 100_000_000_000 {
        // 100 ERG
        return 6;
    } else if nano >= 10_000_000_000 {
        // 10 ERG
        return 5;
    } else if nano >= 1_000_000_000 {
        // 1 ERG {
        return 4;
    } else if nano >= 100_000_000 {
        // 0.1 ERG
        return 3;
    } else if nano >= 10_000_000 {
        // 0.01 ERG
        return 2;
    } else if nano >= 1_000_000 {
        // 0.001 ERG
        return 1;
    }
    // 0 ERG
    0
}

#[cfg(test)]
mod tests {
    use crate::core::types::AddressID;

    use super::super::Balance;
    use super::*;
    use pretty_assertions::assert_eq;

    const ERG: NanoERG = 1_000_000_000;

    #[test]
    fn test_index() {
        // Check balance-to-index mapping
        assert_eq!(index(0), 0);
        assert_eq!(index(1), 0);
        assert_eq!(index(10), 0);
        assert_eq!(index(100), 0);
        assert_eq!(index(1000), 0);
        assert_eq!(index(10_000), 0);
        assert_eq!(index(100_000), 0);
        assert_eq!(index(1_000_000), 1); // 0.001 ERG
        assert_eq!(index(10_000_000), 2); // 0.01 ERG
        assert_eq!(index(100_000_000), 3); // 0.1 ERG
        assert_eq!(index(1 * ERG), 4); // 1 ERG
        assert_eq!(index(10 * ERG), 5); // 10 ERG
        assert_eq!(index(100 * ERG), 6); // 100 ERG
        assert_eq!(index(1000 * ERG), 7); // 1k ERG
        assert_eq!(index(10_000 * ERG), 8); // 10k ERG
        assert_eq!(index(100_000 * ERG), 9); // 100k ERG
        assert_eq!(index(1_000_000 * ERG), 10); // 1M ERG
    }

    #[test]
    fn test_no_changes() {
        let initial = AddressCountsRecord {
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
        let balance_changes = vec![];
        let res = count(&initial, &balance_changes, AddressType::P2PK);
        let expected = AddressCountsRecord {
            height: initial.height + 1,
            total: initial.total,
            ge_0p001: initial.ge_0p001,
            ge_0p01: initial.ge_0p01,
            ge_0p1: initial.ge_0p1,
            ge_1: initial.ge_1,
            ge_10: initial.ge_10,
            ge_100: initial.ge_100,
            ge_1k: initial.ge_1k,
            ge_10k: initial.ge_10k,
            ge_100k: initial.ge_100k,
            ge_1m: initial.ge_1m,
        };
        assert_eq!(res, expected);
    }

    #[test]
    fn test_single_new() {
        // Case with 1 new address
        let initial = AddressCountsRecord {
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
        let balance_changes = vec![BalanceChange {
            address_id: AddressID::dummy(123),
            address_type: AddressType::P2PK,
            old: Bal::Spent,
            new: Bal::Unspent(Balance::new(1000 * ERG, 0)),
        }];
        let res = count(&initial, &balance_changes, AddressType::P2PK);
        let expected = AddressCountsRecord {
            height: initial.height + 1,
            total: 10_001,
            ge_0p001: 9001,
            ge_0p01: 8001,
            ge_0p1: 7001,
            ge_1: 6001,
            ge_10: 5001,
            ge_100: 4001,
            ge_1k: 3001,
            ge_10k: 2000,
            ge_100k: 1000,
            ge_1m: 0,
        };
        assert_eq!(res, expected);
    }

    #[test]
    fn test_single_spent() {
        // Case with 1 spent address
        let initial = AddressCountsRecord {
            height: 100_000,
            total: 10_001,
            ge_0p001: 9001,
            ge_0p01: 8001,
            ge_0p1: 7001,
            ge_1: 6001,
            ge_10: 5001,
            ge_100: 4001,
            ge_1k: 3001,
            ge_10k: 2000,
            ge_100k: 1000,
            ge_1m: 0,
        };
        let balance_changes = vec![BalanceChange {
            address_id: AddressID::dummy(123),
            address_type: AddressType::P2PK,
            old: Bal::Unspent(Balance::new(1000 * ERG, 0)),
            new: Bal::Spent,
        }];
        let res = count(&initial, &balance_changes, AddressType::P2PK);
        let expected = AddressCountsRecord {
            height: initial.height + 1,
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
        assert_eq!(res, expected);
    }

    #[test]
    fn test_single_increase() {
        // Case with 1 existing address having its balance increased
        let initial = AddressCountsRecord {
            height: 100_000,
            total: 10_001,
            ge_0p001: 9001,
            ge_0p01: 8001,
            ge_0p1: 7001,
            ge_1: 6001,
            ge_10: 5000,
            ge_100: 4000,
            ge_1k: 3000,
            ge_10k: 2000,
            ge_100k: 1000,
            ge_1m: 0,
        };
        let balance_changes = vec![BalanceChange {
            address_id: AddressID::dummy(123),
            address_type: AddressType::P2PK,
            old: Bal::Unspent(Balance::new(9 * ERG, 0)),
            new: Bal::Unspent(Balance::new(99_999 * ERG, 0)),
        }];
        let res = count(&initial, &balance_changes, AddressType::P2PK);
        let expected = AddressCountsRecord {
            height: initial.height + 1,
            total: 10_001,
            ge_0p001: 9001,
            ge_0p01: 8001,
            ge_0p1: 7001,
            ge_1: 6001,
            ge_10: 5001,
            ge_100: 4001,
            ge_1k: 3001,
            ge_10k: 2001,
            ge_100k: 1000,
            ge_1m: 0,
        };
        assert_eq!(res, expected);
    }

    #[test]
    fn test_single_decrease() {
        // Case with 1 existing address having its balance decreased
        let initial = AddressCountsRecord {
            height: 100_000,
            total: 10_001,
            ge_0p001: 9001,
            ge_0p01: 8001,
            ge_0p1: 7001,
            ge_1: 6001,
            ge_10: 5001,
            ge_100: 4001,
            ge_1k: 3001,
            ge_10k: 2001,
            ge_100k: 1000,
            ge_1m: 0,
        };
        let balance_changes = vec![BalanceChange {
            address_id: AddressID::dummy(123),
            address_type: AddressType::P2PK,
            old: Bal::Unspent(Balance::new(99_999 * ERG, 0)),
            new: Bal::Unspent(Balance::new(999_999, 0)), // 0.00099999 ERG (less than 0.001 ERG)
        }];
        let res = count(&initial, &balance_changes, AddressType::P2PK);
        let expected = AddressCountsRecord {
            height: initial.height + 1,
            total: 10_001,
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
        assert_eq!(res, expected);
    }
}

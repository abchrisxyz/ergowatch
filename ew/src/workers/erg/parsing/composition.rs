use super::super::types::CompositionRecord;
use crate::constants::address_ids::EMISSION_CONTRACTS;
use crate::core::types::AddressType;
use crate::workers::erg_diffs::types::DiffRecord;

pub(super) fn derive_record(
    cache: &CompositionRecord,
    diffs: &Vec<DiffRecord>,
) -> CompositionRecord {
    let mut next = cache.clone();
    next.height += 1;
    for diff in diffs {
        // Skip emission contracts
        if EMISSION_CONTRACTS.contains(&diff.address_id) {
            continue;
        }
        match diff.address_id.address_type() {
            AddressType::P2PK => next.p2pks += diff.nano,
            AddressType::Other => next.contracts += diff.nano,
            AddressType::Miner => next.miners += diff.nano,
        }
    }
    next
}

#[cfg(test)]
mod tests {
    use super::super::AddressID;
    use super::*;

    #[test]
    fn test_simple_p2pk() {
        let cache = CompositionRecord {
            height: 1000,
            p2pks: 2000,
            contracts: 3000,
            miners: 4000,
        };
        let diffs: Vec<DiffRecord> = vec![DiffRecord::new(AddressID::p2pk(123), 1001, 0, 500)];
        let rec = derive_record(&cache, &diffs);
        assert_eq!(rec.height, 1001);
        assert_eq!(rec.p2pks, 2500);
        assert_eq!(rec.contracts, 3000);
        assert_eq!(rec.miners, 4000);
    }

    #[test]
    fn test_with_emission_addresses() {
        let cache = CompositionRecord {
            height: 1000,
            p2pks: 2000,
            contracts: 3000,
            miners: 4000,
        };
        let diffs: Vec<DiffRecord> = vec![
            DiffRecord::new(AddressID::p2pk(123), 1001, 0, 500),
            DiffRecord::new(EMISSION_CONTRACTS[0], 1001, 0, -75),
            DiffRecord::new(AddressID::miner(456), 1001, 0, 67),
            DiffRecord::new(AddressID::other(789), 1001, 0, -300),
        ];
        let rec = derive_record(&cache, &diffs);
        assert_eq!(rec.height, 1001);
        assert_eq!(rec.p2pks, 2500);
        assert_eq!(rec.contracts, 2700);
        assert_eq!(rec.miners, 4067);
    }
}

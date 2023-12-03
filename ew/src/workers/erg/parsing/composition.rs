use super::super::types::CompositionRecord;
use super::TypedDiff;
use crate::constants::address_ids::EMISSION_CONTRACTS;
use crate::core::types::AddressType;

pub(super) fn derive_record(
    cache: &CompositionRecord,
    typed_diffs: &Vec<TypedDiff>,
) -> CompositionRecord {
    let mut next = cache.clone();
    next.height += 1;
    for diff in typed_diffs {
        // Skip emission contracts
        if EMISSION_CONTRACTS.contains(&diff.record.address_id) {
            continue;
        }
        match diff.address_type {
            AddressType::P2PK => next.p2pks += diff.record.nano,
            AddressType::Other => next.contracts += diff.record.nano,
            AddressType::Miner => next.miners += diff.record.nano,
        }
    }
    next
}

#[cfg(test)]
mod tests {
    use super::super::AddressID;
    use super::super::DiffRecord;
    use super::*;

    #[test]
    fn test_simple_p2pk() {
        let cache = CompositionRecord {
            height: 1000,
            p2pks: 2000,
            contracts: 3000,
            miners: 4000,
        };
        let typed_diffs: Vec<TypedDiff> = vec![TypedDiff::new(
            DiffRecord::new(AddressID::dummy(123), 1001, 0, 500),
            AddressType::P2PK,
        )];
        let rec = derive_record(&cache, &typed_diffs);
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
        let typed_diffs: Vec<TypedDiff> = vec![
            TypedDiff::new(
                DiffRecord::new(AddressID::dummy(123), 1001, 0, 500),
                AddressType::P2PK,
            ),
            TypedDiff::new(
                DiffRecord::new(EMISSION_CONTRACTS[0], 1001, 0, -75),
                AddressType::Other,
            ),
            TypedDiff::new(
                DiffRecord::new(AddressID::dummy(456), 1001, 0, 67),
                AddressType::Miner,
            ),
            TypedDiff::new(
                DiffRecord::new(AddressID::dummy(789), 1001, 0, -300),
                AddressType::Other,
            ),
        ];
        let rec = derive_record(&cache, &typed_diffs);
        assert_eq!(rec.height, 1001);
        assert_eq!(rec.p2pks, 2500);
        assert_eq!(rec.contracts, 2700);
        assert_eq!(rec.miners, 4067);
    }
}

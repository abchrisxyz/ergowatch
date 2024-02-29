use itertools::Itertools;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::constants::address_ids::FEES;
use crate::core::types::AddressID;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::framework::StampedData;
use crate::workers::erg_diffs::types::DiffData;
use crate::workers::erg_diffs::types::DiffRecord;
use crate::workers::erg_diffs::types::SupplyDiff;

use super::types::Batch;
use super::types::DepositAddressConflict;
use super::types::DepositAddressRecord;
use super::types::ExchangeID;
use super::types::InterBlockDepositConflict;
use super::types::IntraBlockDepositConflict;
use super::types::SupplyRecord;

pub struct ParserCache {
    pub supply: SupplyRecord,
    pub main_addresses: HashMap<AddressID, ExchangeID>,
    pub deposit_addresses: HashMap<AddressID, ExchangeID>,
    pub deposit_conflicts: HashMap<AddressID, Option<ExchangeID>>,
    pub deposit_ignored: HashSet<AddressID>,
}

impl ParserCache {
    /// Updates cache to reflect spottings
    fn add_spottings(&mut self, spottings: &Spottings) {
        // Add new deposit addresses
        for (k, v) in &spottings.new_deposits {
            match self.deposit_addresses.insert(*k, *v) {
                None => (),
                Some(_) => panic!("parser cache already contains new deposit address {k:?}"),
            };
        }

        // Add new deposit conflicts
        for (address_id, first_cex_id) in &spottings.inter_conflicts {
            match self
                .deposit_conflicts
                .insert(*address_id, Some(*first_cex_id))
            {
                None => (),
                Some(existing_cex_id) => {
                    tracing::error!(
                        "duplicate inter-block deposit conflict for {:?} - existing cex id: {:?}",
                        address_id,
                        existing_cex_id
                    );
                    panic!("parser cache already contains inter-block deposit conflict for {address_id:?}")
                }
            }
        }
        for address_id in &spottings.intra_conflicts {
            match self.deposit_conflicts.insert(*address_id, None) {
                None => (),
                Some(_) => panic!(
                    "parser cache already contains intra-block deposit conflict {address_id:?}"
                ),
            }
        }
    }
}

pub struct Parser {
    cache: ParserCache,
}

impl Parser {
    pub fn new(cache: ParserCache) -> Self {
        Self { cache }
    }

    pub(super) fn extract_batch(
        &mut self,
        stamped_data: &StampedData<DiffData>,
        spottings: Spottings,
        pos_patch: Vec<SupplyDiff>,
        neg_patch: Vec<SupplyDiff>,
    ) -> StampedData<Batch> {
        let diffs = &stamped_data.data.diff_records;
        let main_supply_diff: NanoERG = diffs
            .iter()
            .filter(|r| self.cache.main_addresses.contains_key(&r.address_id))
            .map(|r| r.nano)
            .sum();

        let deposits_supply_diff: NanoERG = diffs
            .iter()
            .filter(|r| self.cache.deposit_addresses.contains_key(&r.address_id))
            .map(|r| r.nano)
            .sum();

        let unpatched_supply_record = SupplyRecord {
            height: stamped_data.height,
            main: self.cache.supply.main + main_supply_diff,
            deposits: self.cache.supply.deposits + deposits_supply_diff,
        };

        // Merge positive and negative deposit supply patches
        let supply_patch = calculate_net_supply_patch(pos_patch, neg_patch);

        // As it is, the supply record only accounts for changes from deposits
        // spotted in earlier blocks, not this one.
        // Here, we apply the patch to the current supply record.
        let diff: NanoERG = supply_patch.iter().map(|sd| sd.nano).sum();
        let patched_supply_record = SupplyRecord {
            height: unpatched_supply_record.height,
            main: unpatched_supply_record.main,
            deposits: unpatched_supply_record.deposits + diff,
        };
        tracing::trace!("patched supply record: {patched_supply_record:?}");

        // Update cache
        self.cache.supply = patched_supply_record.clone();
        self.cache.add_spottings(&spottings);

        // Convert spottings to batch records
        let deposit_addresses = spottings
            .new_deposits
            .into_iter()
            .map(|(address_id, cex_id)| DepositAddressRecord {
                address_id,
                cex_id,
                spot_height: stamped_data.height,
            })
            .collect();

        // Convert inter- and intra-block conflicting addresses into conflicts
        let deposit_conflicts = spottings
            .inter_conflicts
            .into_iter()
            .map(|(address_id, first_cex_id)| {
                DepositAddressConflict::Inter(InterBlockDepositConflict {
                    address_id: address_id,
                    first_cex_id: first_cex_id,
                    conflict_spot_height: stamped_data.height,
                })
            })
            .chain(spottings.intra_conflicts.into_iter().map(|address_id| {
                DepositAddressConflict::Intra(IntraBlockDepositConflict {
                    address_id: address_id,
                    conflict_spot_height: stamped_data.height,
                })
            }))
            .collect();

        stamped_data.wrap(Batch {
            supply: unpatched_supply_record,
            supply_patch,
            deposit_addresses,
            deposit_conflicts,
        })
    }

    pub(super) fn spot_deposit_addresses(&self, stamped_data: &StampedData<DiffData>) -> Spottings {
        spot_deposit_addresses(&stamped_data.data.diff_records, &self.cache)
    }
}

pub struct Spottings {
    /// Maps new deposit addresses to their cex.
    pub new_deposits: HashMap<AddressID, ExchangeID>,
    /// Maps addresses to the first cex they were soptted as deposit for.
    pub inter_conflicts: HashMap<AddressID, ExchangeID>,
    /// Collection of addresses spotted as deposit and conflict within same block.
    pub intra_conflicts: HashSet<AddressID>,
}

pub(super) fn spot_deposit_addresses(diffs: &Vec<DiffRecord>, cache: &ParserCache) -> Spottings {
    // Detect new deposit addresses
    // Get idx of txs sending to a main
    let candidate_txs: HashSet<i16> = HashSet::from_iter(
        diffs
            .iter()
            .filter(|r| r.nano > 0 && cache.main_addresses.contains_key(&r.address_id))
            .map(|r| r.tx_idx),
    );

    // Inter-block conflicts
    // A conflict is a known deposit address found sending to a different cex.
    // Candidates conflicting with an existing deposit addresses (need patching).
    let mut inter_conflicts: HashMap<AddressID, ExchangeID> = HashMap::new();
    // Intra-block conflicts
    // Candidates conflicting with new deposit addresses only (no patching needed).
    let mut intra_conflicts: HashSet<AddressID> = HashSet::new();
    // Candidate deposit addresses.
    // A temporary hashmap collecting new candidates and their exchange.
    // We buffer candidates here first to distinguisg between existing
    // and new deposit addresses and, in turn, distinguish between inter
    // and intra-block conflicts.
    let mut new_deposits: HashMap<AddressID, ExchangeID> = HashMap::new();

    // Group diffs of candidate txs
    let candidate_diffs: Vec<&DiffRecord> = diffs
        .iter()
        .filter(|r| candidate_txs.contains(&r.tx_idx))
        .collect();
    for tx_idx in &candidate_txs {
        let tx_diffs: Vec<&DiffRecord> = candidate_diffs
            .iter()
            .filter(|r| r.tx_idx == *tx_idx && r.address_id != FEES)
            .map(|r| *r)
            .collect();

        // Collect receiving exchanges.
        let receiving_exchanges: Vec<&ExchangeID> = tx_diffs
            .iter()
            .filter(|r| r.nano > 0)
            .filter_map(|r| cache.main_addresses.get(&r.address_id))
            .unique()
            .collect();

        // Collect candidate deposit addreses.
        // Those are sending to, but are not, a main exchange address.
        let candidate_addresses: Vec<AddressID> = tx_diffs
            .iter()
            .filter(|r| r.nano < 0 && !cache.main_addresses.contains_key(&r.address_id))
            .map(|r| r.address_id)
            // Drop any known conflicting and ignored addresses
            .filter(|a| {
                !cache.deposit_conflicts.contains_key(a) && !cache.deposit_ignored.contains(a)
            })
            .collect();

        // Check number of receiving cex's (more than 1 = conclict)
        if receiving_exchanges.len() > 1 {
            // More than 1 exchange involved so all sending addresses have to be excluded.
            for candidate_address_id in candidate_addresses {
                // Check if it is also an inter-block conflict
                match cache.deposit_addresses.get(&candidate_address_id) {
                    Some(cex_id) => {
                        // This is an inter-block conflict
                        inter_conflicts.insert(candidate_address_id, *cex_id);
                    }
                    None => {
                        // Not an inter-block conflict, so record it as intra
                        intra_conflicts.insert(candidate_address_id);
                    }
                }
            }

            // Proceed with next tx
            continue;
        }

        // Always expecting exactly 1 exchange at this point
        let exchange_id = *receiving_exchanges[0];

        // Check if conflicting with existing records
        for candidate_address_id in candidate_addresses {
            // Check against spottings from previous blocks
            match cache.deposit_addresses.get(&candidate_address_id) {
                None => {
                    // New address
                    // Now also check against spottings from previous txs in same block
                    match new_deposits.get(&candidate_address_id) {
                        None => {
                            // Indeed a new address - add it to final collection of candidates
                            new_deposits.insert(candidate_address_id, exchange_id);
                        }
                        Some(known_exchange_id) => {
                            // Address already spotted in a previous tx
                            // Check it matches current spotting
                            if exchange_id != *known_exchange_id {
                                // Found an intra-block conflict
                                intra_conflicts.insert(candidate_address_id);
                            }
                        }
                    }
                }
                Some(existing_exchange_id) => {
                    // Address is already linked to an exchange.
                    // Check it matches current spotting.
                    if exchange_id != *existing_exchange_id {
                        // Found a conflict
                        inter_conflicts.insert(candidate_address_id, *existing_exchange_id);
                    }
                }
            }
        }
    }

    // Weed out conflicts from candidates.
    for conflicting_address_id in inter_conflicts.keys() {
        new_deposits.remove(conflicting_address_id);
    }
    for conflicting_address_id in &intra_conflicts {
        new_deposits.remove(conflicting_address_id);
    }

    Spottings {
        new_deposits,
        inter_conflicts,
        intra_conflicts,
    }
}

/// Combines supply diffs to be added and substracted into one net supply diffs patch.
pub(super) fn calculate_net_supply_patch(
    pos: Vec<SupplyDiff>,
    neg: Vec<SupplyDiff>,
) -> Vec<SupplyDiff> {
    // Negative patches will be relatively rare (inetr-block conflicts only).
    // If none, just return positive patch as is.
    if neg.is_empty() {
        return pos;
    }
    let mut patch: HashMap<Height, NanoERG> =
        HashMap::from_iter(pos.into_iter().map(|d| (d.height, d.nano)));
    for sub_diff in neg {
        patch
            .entry(sub_diff.height)
            .and_modify(|nano| *nano -= sub_diff.nano)
            .or_insert(-sub_diff.nano);
    }

    patch
        .into_iter()
        .map(|(h, n)| SupplyDiff::new(h, n))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const CEX1: i32 = 1;
    const CEX2: i32 = 2;

    #[test]
    fn test_spot_deposit_addresses_simple() {
        let cex_1 = AddressID(5551);
        let diffs = vec![
            DiffRecord::new(AddressID(1231), 5, 0, -100),
            DiffRecord::new(cex_1, 5, 0, 100),
        ];
        let cache = ParserCache {
            supply: SupplyRecord {
                height: 5,
                main: 100000,
                deposits: 1000,
            },
            main_addresses: HashMap::from([(cex_1, 100_000)]),
            deposit_addresses: HashMap::new(),
            deposit_conflicts: HashMap::new(),
            deposit_ignored: HashSet::new(),
        };
        let spottings = spot_deposit_addresses(&diffs, &cache);
        assert_eq!(spottings.new_deposits.len(), 1);
        assert_eq!(spottings.inter_conflicts.len(), 0);
        assert_eq!(spottings.intra_conflicts.len(), 0);
    }

    #[test]
    fn test_spot_deposit_addresses_inter_conflicts() {
        // Some main cex addresses
        let cex_1a = AddressID(1011);
        let cex_1b = AddressID(1021);
        let cex_2 = AddressID(2011);
        // An existing deposit address, linked to cex 1
        let dep_1 = AddressID(1231);
        // Two other deposit addresses, not linked yet
        let dep_2 = AddressID(4561);
        let dep_3 = AddressID(7891);

        let diffs = vec![
            // Here, dep_1 send to cex 2 --> inter conflict
            DiffRecord::new(dep_1, 5, 0, -100),
            DiffRecord::new(cex_2, 5, 0, 100),
            // A tx with dep_2 sending to 2 cex's --> 1st intra conflict
            DiffRecord::new(dep_2, 5, 1, -200),
            DiffRecord::new(cex_1a, 5, 1, 100),
            DiffRecord::new(cex_2, 5, 1, 100),
            // Two tx's, each with dep_3 sending to a different cex -- 2nd intra conflict
            DiffRecord::new(dep_3, 5, 2, -100),
            DiffRecord::new(cex_1a, 5, 2, 100),
            DiffRecord::new(dep_3, 5, 3, -100),
            DiffRecord::new(cex_2, 5, 3, 100),
        ];
        let cache = ParserCache {
            supply: SupplyRecord {
                height: 5,
                main: 100000,
                deposits: 1000,
            },
            main_addresses: HashMap::from([(cex_1a, CEX1), (cex_1b, CEX1), (cex_2, CEX2)]),
            deposit_addresses: HashMap::from([(dep_1, CEX1)]),
            deposit_conflicts: HashMap::new(),
            deposit_ignored: HashSet::new(),
        };
        let spottings = spot_deposit_addresses(&diffs, &cache);

        assert_eq!(spottings.new_deposits.len(), 0);
        assert_eq!(spottings.inter_conflicts.len(), 1);
        assert_eq!(spottings.intra_conflicts.len(), 2);

        assert_eq!(spottings.inter_conflicts.get(&dep_1), Some(&CEX1));

        assert!(spottings.intra_conflicts.contains(&dep_2));
        assert!(spottings.intra_conflicts.contains(&dep_3));
    }

    #[test]
    fn test_spot_deposit_ignores_known_conflicts() {
        // Some main cex address
        let cex_1 = AddressID(1011);
        // An existing conflict address
        let dep_1 = AddressID(1231);
        // An ignored address
        let dep_2 = AddressID(4561);

        let diffs = vec![
            // dep_1 sends to cex 1 --> should be ignored
            DiffRecord::new(dep_1, 5, 0, -100),
            DiffRecord::new(cex_1, 5, 0, 100),
            // dep_2 sends to cex 1 --> should be ignored
            DiffRecord::new(dep_2, 5, 1, -100),
            DiffRecord::new(cex_1, 5, 1, 100),
        ];
        let cache = ParserCache {
            supply: SupplyRecord {
                height: 5,
                main: 100000,
                deposits: 1000,
            },
            main_addresses: HashMap::from([(cex_1, CEX1)]),
            deposit_addresses: HashMap::new(),
            deposit_conflicts: HashMap::from([(dep_1, Some(CEX1))]),
            deposit_ignored: HashSet::from([dep_2]),
        };
        let spottings = spot_deposit_addresses(&diffs, &cache);

        assert_eq!(spottings.new_deposits.len(), 0);
        assert_eq!(spottings.inter_conflicts.len(), 0);
        assert_eq!(spottings.intra_conflicts.len(), 0);
    }
}

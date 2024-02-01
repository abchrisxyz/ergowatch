mod difficulty_cache;

use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;

use super::types::Batch;
use super::types::BatchData;
use super::types::Difficulty;
use super::types::ExtensionField;
use super::types::MiningRecord;
use super::types::NetworkParameter;
use super::types::NetworkParametersRecord;
use super::types::NetworkParametersRecordBuilder;
use super::types::Proposal;
use super::types::ProposalRecord;
use super::types::TransactionsRecord;
use super::types::UnhandledExtensionRecord;
use super::types::VotesRecord;
use crate::constants::address_ids::EMISSION;
use crate::constants::address_ids::FEES;
use crate::constants::address_ids::REEMISSION;
use crate::constants::VOTING_EPOCH_LENGTH;
use crate::core::types::AddressID;
use crate::core::types::BlockHeader;
use crate::core::types::CoreData;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;
use crate::core::types::Transaction;
use crate::framework::StampedData;
use difficulty_cache::DifficultyCache;

pub(super) struct ParserCache {
    /// Current running proposal, if any.
    current_proposal: Proposal,

    /// Last 24h of difficulty
    difficulty: DifficultyCache,
}

impl ParserCache {
    pub fn new(
        last_proposal_record: Option<ProposalRecord>,
        difficulties: Vec<(Timestamp, Difficulty)>,
    ) -> Self {
        Self {
            current_proposal: last_proposal_record.into(),
            difficulty: DifficultyCache::new(difficulties),
        }
    }
}

pub(super) struct Parser {
    cache: ParserCache,
}

impl Parser {
    pub fn new(cache: ParserCache) -> Self {
        Self { cache }
    }

    pub(super) fn extract_batch(
        &mut self,
        stamped_data: &StampedData<CoreData>,
    ) -> StampedData<Batch> {
        let block = &stamped_data.data.block;
        let height = block.header.height;

        if height == 0 {
            // No data for genesis
            return stamped_data.wrap(Batch::Genesis);
        }

        let extension_fields: Vec<ExtensionField> = block
            .extension
            .fields
            .iter()
            .map(|f| ExtensionField::from_bytes(&f.key, &f.value))
            .collect();

        let difficulty = Decimal::from_str(&block.header.difficulty).unwrap();
        let miner_address_id = extract_miner_address_id(&block.transactions);
        let reward = extract_reward(&block.transactions);

        // Add new difficulty to cache before calculating hash rate
        self.cache
            .difficulty
            .push((block.header.timestamp, difficulty));
        let hash_rate_24h_mean = self.cache.difficulty.calculate_hash_rate();
        let difficulty_24h_mean = self.cache.difficulty.calculate_daily_mean_difficulty();

        // Proposal
        let proposal = extract_proposal(
            &block.header,
            miner_address_id,
            &self.cache.current_proposal,
        );
        self.cache.current_proposal = proposal;

        let batch = Batch::Block(BatchData {
            parameters: extract_paramaters(height, &extension_fields),
            votes: extract_votes(&block.header),
            proposal: self.cache.current_proposal.clone(),
            mining: MiningRecord {
                height: block.header.height,
                miner_address_id,
                difficulty,
                difficulty_24h_mean,
                hash_rate_24h_mean,
                block_reward: reward,
                tx_fees: extract_fees(&block.transactions),
            },
            unhandled_extensions: extension_fields
                .iter()
                .filter_map(|f| match f {
                    ExtensionField::Unknown(key, val) => Some(UnhandledExtensionRecord {
                        height: block.header.height,
                        key: *key as i16,
                        value: val.to_owned(),
                    }),
                    _ => None,
                })
                .collect(),
            transactions: TransactionsRecord {
                height: block.header.height,
                transactions: block.transactions.len() as i32,
                user_transactions: count_user_transactions(&block.transactions),
            },
        });

        stamped_data.wrap(batch)
    }
}

/// Extract network parameters from extension fields, if present.
fn extract_paramaters(
    height: Height,
    fields: &Vec<ExtensionField>,
) -> Option<NetworkParametersRecord> {
    let params: Vec<&NetworkParameter> = fields
        .iter()
        .filter_map(|f| match f {
            ExtensionField::Parameter(p) => Some(p),
            _ => None,
        })
        .collect();

    if params.is_empty() {
        return None;
    }

    let record = params
        .into_iter()
        .fold(NetworkParametersRecordBuilder::new(height), |builder, p| {
            builder.set(p)
        })
        .build();
    Some(record)
}

/// Extract a votes record from a block header.
fn extract_votes(header: &BlockHeader) -> VotesRecord {
    VotesRecord {
        height: header.height,
        slot1: header.votes[0] as i16,
        slot2: header.votes[1] as i16,
        slot3: header.votes[2] as i16,
    }
}

fn extract_proposal(
    header: &BlockHeader,
    miner_address_id: AddressID,
    current_proposal: &Proposal,
) -> Proposal {
    match header.height % VOTING_EPOCH_LENGTH {
        0 => {
            // New voting epoch, so possible new proposal
            match header.votes {
                [0, 0, 0] => Proposal::Empty,
                _ => Proposal::New(ProposalRecord {
                    epoch: header.height / VOTING_EPOCH_LENGTH,
                    height: header.height,
                    miner_address_id,
                    slots: [
                        header.votes[0] as i16,
                        header.votes[1] as i16,
                        header.votes[2] as i16,
                    ],
                    tally: [0, 0, 0],
                }),
            }
        }
        _ => {
            // Voting epoch started already, tally votes
            match current_proposal {
                Proposal::New(r) | Proposal::Tally(r) => {
                    let rec = r.with_votes(header.votes);
                    Proposal::Tally(rec)
                }
                Proposal::Empty => Proposal::Empty,
            }
        }
    }
}

/// Return transaction paying block reward to miner, if any.
fn extract_reward_transaction(transactions: &[Transaction]) -> Option<&Transaction> {
    let emission_txs: Vec<&Transaction> = transactions
        .iter()
        .filter(|tx| {
            tx.inputs
                .iter()
                .filter(|bx| bx.address_id == EMISSION || bx.address_id == REEMISSION)
                .peekable()
                .peek()
                .is_some()
        })
        .filter(|tx| {
            tx.outputs
                .iter()
                .filter(|bx| bx.address_id.is_miner())
                .peekable()
                .peek()
                .is_some()
        })
        .collect();

    // Never expecting more than 1 emission transaction.
    assert!(emission_txs.len() <= 1);

    match emission_txs.first() {
        Some(tx) => Some(*tx),
        // Some
        None => None,
    }
}

/// Returns address id receiving the block reward or,
/// alternatively, collecting tx fees.
fn extract_miner_address_id(transactions: &[Transaction]) -> AddressID {
    let emission_tx = extract_reward_transaction(&transactions);

    let miner_tx = match emission_tx {
        Some(tx) => tx,
        None => {
            // If no emission payout tx, look for fee collection tx.
            let fee_collection_txs: Vec<&Transaction> = transactions
                .iter()
                .filter(|tx| {
                    tx.inputs
                        .iter()
                        .filter(|bx| bx.address_id == FEES)
                        .peekable()
                        .peek()
                        .is_some()
                })
                .collect();
            assert_eq!(fee_collection_txs.len(), 1);
            fee_collection_txs[0]
        }
    };

    let miner_candidates: Vec<AddressID> = miner_tx
        .outputs
        .iter()
        .filter(|bx| bx.address_id.is_miner())
        .map(|bx| bx.address_id)
        .collect();
    // Not expecting more than 1 miner
    assert_eq!(miner_candidates.len(), 1);
    miner_candidates[0]
}

/// Return block reward collected by miner, if any
fn extract_reward(transactions: &[Transaction]) -> NanoERG {
    let reward_tx = extract_reward_transaction(transactions);
    match reward_tx {
        Some(tx) => {
            let mut miner_diffs = tx.non_zero_diffs();
            miner_diffs.retain(|k, v| *v > 0 && k.is_miner());
            assert_eq!(miner_diffs.len(), 1);
            miner_diffs.into_values().next().unwrap()
        }
        None => 0,
    }
}

fn extract_fees(transactions: &[Transaction]) -> NanoERG {
    transactions
        .iter()
        .map(|tx| {
            tx.outputs
                .iter()
                .filter(|bx| bx.address_id == FEES)
                .map(|bx| bx.value)
                .collect::<Vec<NanoERG>>()
        })
        .flatten()
        .sum()
}

/// Returns number of user transactions (not related to block's miner).
///
/// Ignores block rewards and fee collections
fn count_user_transactions(transactions: &[Transaction]) -> i32 {
    transactions
        .iter()
        .filter(|tx| {
            tx.inputs
                .iter()
                .filter(|bx| {
                    bx.address_id == FEES
                        || bx.address_id == EMISSION
                        || bx.address_id == REEMISSION
                })
                .peekable()
                .peek()
                .is_none()
        })
        .count() as i32
}

#[cfg(test)]
mod tests {
    use crate::core::types::{Block, BoxData};

    use super::*;

    #[test]
    fn test_extract_miner_from_emission() {
        let miner = AddressID::miner(500);
        let txs = vec![Transaction::dummy()
            .add_input(BoxData::dummy().address_id(EMISSION).value(130_000_000_000))
            .add_output(BoxData::dummy().address_id(EMISSION).value(127_000_000_000))
            .add_output(BoxData::dummy().address_id(miner).value(3_000_000_000))];
        assert_eq!(extract_miner_address_id(&txs), miner);
    }

    #[test]
    fn test_extract_miner_from_fee_collection() {
        let miner = AddressID::miner(500);
        let txs = vec![Transaction::dummy()
            .add_input(BoxData::dummy().address_id(FEES).value(100_000_000))
            .add_output(BoxData::dummy().address_id(miner).value(100_000_000))];
        assert_eq!(extract_miner_address_id(&txs), miner);
    }

    #[test]
    fn test_extract_reward_from_emission() {
        let miner = AddressID::miner(500);
        let txs = vec![Transaction::dummy()
            .add_input(BoxData::dummy().address_id(EMISSION).value(130_000_000_000))
            .add_output(BoxData::dummy().address_id(EMISSION).value(127_000_000_000))
            .add_output(BoxData::dummy().address_id(miner).value(3_000_000_000))];
        assert_eq!(extract_reward(&txs), 3_000_000_000);
    }

    #[test]
    fn test_extract_reward_from_reemission() {
        let miner = AddressID::miner(500);
        let txs = vec![Transaction::dummy()
            .add_input(
                BoxData::dummy()
                    .address_id(REEMISSION)
                    .value(130_000_000_000),
            )
            .add_output(
                BoxData::dummy()
                    .address_id(REEMISSION)
                    .value(127_000_000_000),
            )
            .add_output(BoxData::dummy().address_id(miner).value(3_000_000_000))];
        assert_eq!(extract_reward(&txs), 3_000_000_000);
    }

    #[test]
    fn test_extract_reward_no_reward() {
        let txs = vec![Transaction::dummy()
            .add_input(BoxData::dummy().address_id(AddressID(-1)).value(130))
            .add_output(BoxData::dummy().address_id(AddressID(-1)).value(127))
            .add_output(BoxData::dummy().address_id(AddressID(-2)).value(3))];
        assert_eq!(extract_reward(&txs), 0);
    }

    #[test]
    fn test_extract_fees_no_fees() {
        let txs = vec![Transaction::dummy()
            .add_input(BoxData::dummy())
            .add_output(BoxData::dummy())
            .add_output(BoxData::dummy())];
        assert_eq!(extract_fees(&txs), 0);
    }

    #[test]
    fn test_extract_fees_single_fee() {
        let txs = vec![Transaction::dummy()
            .add_input(BoxData::dummy())
            .add_output(BoxData::dummy())
            .add_output(BoxData::dummy().address_id(FEES).value(123))];
        assert_eq!(extract_fees(&txs), 123);
    }

    #[test]
    fn test_extract_fees_multiple_fees() {
        let txs = vec![
            Transaction::dummy()
                .add_input(BoxData::dummy())
                .add_output(BoxData::dummy())
                .add_output(BoxData::dummy().address_id(FEES).value(123)),
            Transaction::dummy()
                .add_input(BoxData::dummy())
                .add_output(BoxData::dummy())
                .add_output(BoxData::dummy().address_id(FEES).value(1000)),
        ];
        assert_eq!(extract_fees(&txs), 1123);
    }

    #[test]
    fn test_count_user_transactions_typical() {
        let miner = AddressID::miner(500);
        let txs = vec![
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(EMISSION).value(130_000_000_000))
                .add_output(BoxData::dummy().address_id(EMISSION).value(127_000_000_000))
                .add_output(BoxData::dummy().address_id(miner).value(3_000_000_000)),
            Transaction::dummy()
                .add_input(BoxData::dummy().value(2000_000_000_000))
                .add_output(BoxData::dummy().value(1999_000_000_000))
                .add_output(BoxData::dummy().address_id(FEES).value(1_000_000_000)),
            Transaction::dummy()
                .add_input(BoxData::dummy().value(4000_000_000_000))
                .add_output(BoxData::dummy().value(3998_000_000_000))
                .add_output(BoxData::dummy().address_id(FEES).value(2_000_000_000)),
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(FEES).value(1_000_000_000))
                .add_output(BoxData::dummy().address_id(miner).value(1_000_000_000)),
        ];
        assert_eq!(count_user_transactions(&txs), 2);
    }

    #[test]
    fn test_extract_proposal_after_new() {
        let block = Block::dummy().votes([0, 4, 0]);
        let miner = AddressID::miner(500);
        let current_proposal = Proposal::New(ProposalRecord {
            epoch: 100,
            height: 102400,
            miner_address_id: miner.clone(),
            slots: [0, 4, 0],
            tally: [0, 0, 0],
        });
        let proposal = extract_proposal(&block.header, miner.clone(), &current_proposal);
        assert_eq!(
            proposal,
            Proposal::Tally(ProposalRecord {
                epoch: 100,
                height: 102400,
                miner_address_id: miner.clone(),
                slots: [0, 4, 0],
                tally: [0, 1, 0],
            })
        );
    }

    #[test]
    fn test_extract_proposal_after_tally() {
        let block = Block::dummy().votes([0, 4, 0]);
        let miner = AddressID::miner(500);
        let current_proposal = Proposal::Tally(ProposalRecord {
            epoch: 100,
            height: 102400,
            miner_address_id: miner.clone(),
            slots: [0, 4, 0],
            tally: [0, 7, 0],
        });
        let proposal = extract_proposal(&block.header, miner.clone(), &current_proposal);
        assert_eq!(
            proposal,
            Proposal::Tally(ProposalRecord {
                epoch: 100,
                height: 102400,
                miner_address_id: miner.clone(),
                slots: [0, 4, 0],
                tally: [0, 8, 0],
            })
        );
    }
}

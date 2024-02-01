use postgres_from_row::FromRow;
use rust_decimal::Decimal;

use crate::constants::VOTING_EPOCH_LENGTH;
use crate::core::types::AddressID;
use crate::core::types::Height;

pub type Difficulty = Decimal;

pub enum Batch {
    Genesis,
    Block(BatchData),
}

pub struct BatchData {
    pub parameters: Option<NetworkParametersRecord>,
    pub votes: VotesRecord,
    pub proposal: Proposal,
    pub mining: MiningRecord,
    pub unhandled_extensions: Vec<UnhandledExtensionRecord>,
    pub transactions: TransactionsRecord,
}

#[derive(Debug, FromRow)]
pub struct NetworkParametersRecord {
    pub height: Height,
    /// Storage fee nanoErg/byte (1)
    pub storage_fee: i32,
    /// Minimum box value in nanoErg (2)
    pub min_box_value: i32,
    /// Maximum block size (3)
    pub max_block_size: i32,
    /// Maximum computational cost of a block (4)
    pub max_cost: i32,
    /// Token access cost (5)
    pub token_access_cost: i32,
    /// Cost per tx input (6)
    pub tx_input_cost: i32,
    /// Cost per tx data-input (7)
    pub tx_data_input_cost: i32,
    /// Cost per tx output (8)
    pub tx_output_cost: i32,
    /// Block version (123)
    pub block_version: i32,
}

pub struct NetworkParametersRecordBuilder {
    rec: NetworkParametersRecord,
}

impl NetworkParametersRecordBuilder {
    pub fn new(height: Height) -> Self {
        Self {
            rec: NetworkParametersRecord {
                height: height,
                storage_fee: 0,
                min_box_value: 0,
                max_block_size: 0,
                max_cost: 0,
                token_access_cost: 0,
                tx_input_cost: 0,
                tx_data_input_cost: 0,
                tx_output_cost: 0,
                block_version: 0,
            },
        }
    }

    pub fn set(mut self, param: &NetworkParameter) -> Self {
        match param {
            NetworkParameter::Nothing => (),
            NetworkParameter::StorageFee(val) => {
                self.rec.storage_fee = *val;
            }
            NetworkParameter::MinBoxValue(val) => {
                self.rec.min_box_value = *val;
            }
            NetworkParameter::MaxBlockSize(val) => {
                self.rec.max_block_size = *val;
            }
            NetworkParameter::MaxCost(val) => {
                self.rec.max_cost = *val;
            }
            NetworkParameter::TokenAccessCost(val) => {
                self.rec.token_access_cost = *val;
            }
            NetworkParameter::TxInputCost(val) => {
                self.rec.tx_input_cost = *val;
            }
            NetworkParameter::TxDataInputCost(val) => {
                self.rec.tx_data_input_cost = *val;
            }
            NetworkParameter::TxOutputCost(val) => {
                self.rec.tx_output_cost = *val;
            }
            NetworkParameter::BlockVersion(val) => {
                self.rec.block_version = *val;
            }
        }
        self
    }

    pub fn build(self) -> NetworkParametersRecord {
        self.rec
    }
}

#[derive(Debug)]
pub struct VotesRecord {
    pub height: Height,
    pub slot1: i16,
    pub slot2: i16,
    pub slot3: i16,
}

impl VotesRecord {
    /// Returns votes as i8 array
    pub fn pack(&self) -> [i8; 3] {
        [self.slot1 as i8, self.slot2 as i8, self.slot3 as i8]
    }
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "test-utilities", derive(PartialEq, Eq))]
pub enum Proposal {
    /// A newly submitted proposal (possibly proposing nothing)
    New(ProposalRecord),
    /// Existing proposal with updated vote counts
    Tally(ProposalRecord),
    /// No (valid) running proposal
    Empty,
}

impl From<Option<ProposalRecord>> for Proposal {
    fn from(value: Option<ProposalRecord>) -> Self {
        match value {
            Some(record) => {
                if record.height % VOTING_EPOCH_LENGTH == 0 {
                    Self::New(record)
                } else {
                    Self::Tally(record)
                }
            }
            None => Self::Empty,
        }
    }
}

/// A new proposal record.
///
/// Can be empty slots (all slots set to zero)
#[derive(Debug, Clone)]
#[cfg_attr(feature = "test-utilities", derive(PartialEq, Eq))]
pub struct ProposalRecord {
    pub epoch: i32,
    pub height: Height,
    pub miner_address_id: AddressID,
    /// Proposed parameters
    pub slots: [i16; 3],
    /// Number of favorable votes so far
    pub tally: [i16; 3],
}

impl ProposalRecord {
    /// Add votes to tallies if in favor of respoctive proposal slots
    pub fn with_votes(&self, vote_params: [i8; 3]) -> Self {
        let new_tally = [
            self.tally[0] + (self.slots[0] != 0 && self.slots[0] == vote_params[0] as i16) as i16,
            self.tally[1] + (self.slots[1] != 0 && self.slots[1] == vote_params[1] as i16) as i16,
            self.tally[2] + (self.slots[2] != 0 && self.slots[2] == vote_params[2] as i16) as i16,
        ];
        Self {
            epoch: self.epoch,
            height: self.height,
            miner_address_id: self.miner_address_id,
            slots: self.slots,
            tally: new_tally,
        }
    }

    /// Remove votes from tallies if those were in favor of respective proposal slots
    pub fn withdraw_votes(&self, vote_params: [i8; 3]) -> Self {
        let new_tally = [
            self.tally[0] - (self.slots[0] != 0 && self.slots[0] == vote_params[0] as i16) as i16,
            self.tally[1] - (self.slots[1] != 0 && self.slots[1] == vote_params[1] as i16) as i16,
            self.tally[2] - (self.slots[2] != 0 && self.slots[2] == vote_params[2] as i16) as i16,
        ];
        Self {
            epoch: self.epoch,
            height: self.height,
            miner_address_id: self.miner_address_id,
            slots: self.slots,
            tally: new_tally,
        }
    }
}

#[derive(Debug)]
pub struct UnhandledExtensionRecord {
    pub height: Height,
    /// First and second u8 of the key in a single i16
    pub key: i16,
    /// Base16 encoded value
    pub value: String,
}

#[derive(Debug)]
pub struct TransactionsRecord {
    pub height: Height,
    pub transactions: i32,
    pub user_transactions: i32,
}

#[derive(Debug)]
pub struct MiningRecord {
    pub height: Height,
    pub miner_address_id: AddressID,
    pub difficulty: Difficulty,
    pub difficulty_24h_mean: Difficulty,
    pub hash_rate_24h_mean: i64,
    pub block_reward: i64,
    pub tx_fees: i64,
}

/// Block extension field
///
/// https://github.com/ergoplatform/ergo/blob/master/papers/yellow/block.tex
/// https://github.com/ergoplatform/ergo/blob/master/papers/yellow/voting.tex
#[derive(Debug, PartialEq)]
pub enum ExtensionField {
    Parameter(NetworkParameter),
    /// Interlinks vector - ignored for now
    Interlink,
    Unknown(i16, String),
}

impl ExtensionField {
    /// Create a new ExtensionField from base16 encode extension fields.
    pub fn from_bytes(key: &str, val: &str) -> Self {
        let key_bytes = base16::decode(key.as_bytes()).unwrap();
        match key_bytes[0] {
            // First key byte is 0, this is a parameter.
            0 => {
                // Next key byte is a parameter id
                match NetworkParameter::new(key_bytes[1], val) {
                    Some(param) => Self::Parameter(param),
                    None => {
                        let k = i16::from_be_bytes([key_bytes[0], key_bytes[1]]);
                        ExtensionField::Unknown(k, val.to_owned())
                    }
                }
            }
            1 => ExtensionField::Interlink, // Ignore interlink stuff
            _ => {
                let k = i16::from_be_bytes([key_bytes[0], key_bytes[1]]);
                ExtensionField::Unknown(k, val.to_owned())
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum NetworkParameter {
    Nothing,
    StorageFee(i32),
    MinBoxValue(i32),
    MaxBlockSize(i32),
    MaxCost(i32),
    TokenAccessCost(i32),
    TxInputCost(i32),
    TxDataInputCost(i32),
    TxOutputCost(i32),
    BlockVersion(i32),
}

impl NetworkParameter {
    /// New parameter from id byte and base16 encoded value
    pub fn new(id_byte: u8, value: &str) -> Option<Self> {
        let bytes = base16::decode(value.as_bytes()).unwrap();
        if bytes.len() != 4 {
            return None;
        }
        let val = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        match id_byte {
            0 => Some(Self::Nothing),
            1 => Some(Self::StorageFee(val)),
            2 => Some(Self::MinBoxValue(val)),
            3 => Some(Self::MaxBlockSize(val)),
            4 => Some(Self::MaxCost(val)),
            5 => Some(Self::TokenAccessCost(val)),
            6 => Some(Self::TxInputCost(val)),
            7 => Some(Self::TxDataInputCost(val)),
            8 => Some(Self::TxOutputCost(val)),
            123 => Some(Self::BlockVersion(val)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_field() {
        assert_eq!(
            ExtensionField::from_bytes("0000", "00000000"),
            ExtensionField::Parameter(NetworkParameter::Nothing)
        );
        assert_eq!(
            ExtensionField::from_bytes("0001", "001312d0"),
            ExtensionField::Parameter(NetworkParameter::StorageFee(1250000))
        );
        assert_eq!(
            ExtensionField::from_bytes("0002", "00000168"),
            ExtensionField::Parameter(NetworkParameter::MinBoxValue(360))
        );
        assert_eq!(
            ExtensionField::from_bytes("0003", "001364e1"),
            ExtensionField::Parameter(NetworkParameter::MaxBlockSize(1271009))
        );
        assert_eq!(
            ExtensionField::from_bytes("0004", "006b45fc"),
            ExtensionField::Parameter(NetworkParameter::MaxCost(7030268))
        );
        assert_eq!(
            ExtensionField::from_bytes("0005", "00000064"),
            ExtensionField::Parameter(NetworkParameter::TokenAccessCost(100))
        );
        assert_eq!(
            ExtensionField::from_bytes("0006", "000007d0"),
            ExtensionField::Parameter(NetworkParameter::TxInputCost(2000))
        );
        assert_eq!(
            ExtensionField::from_bytes("0007", "00000064"),
            ExtensionField::Parameter(NetworkParameter::TxDataInputCost(100))
        );
        assert_eq!(
            ExtensionField::from_bytes("0008", "00000064"),
            ExtensionField::Parameter(NetworkParameter::TxOutputCost(100))
        );
        assert_eq!(
            ExtensionField::from_bytes("007b", "00000002"),
            ExtensionField::Parameter(NetworkParameter::BlockVersion(2))
        );
        assert_eq!(
            ExtensionField::from_bytes("007c", "0000"),
            ExtensionField::Unknown(124, "0000".to_owned())
        );
        assert_eq!(
            ExtensionField::from_bytes(
                "0100",
                "01b0244dfc267baca974a4caee06120321562784303a8a688976ae56170e4d175b"
            ),
            ExtensionField::Interlink
        );
    }

    #[test]
    pub fn test_proposal_record_with_votes_but_no_proposal() {
        let record = ProposalRecord {
            epoch: 1,
            height: 1024,
            miner_address_id: AddressID::miner(500),
            slots: [0, 0, 0],
            tally: [0, 0, 0],
        };
        assert_eq!(record.with_votes([0, 1, 0]), record);
    }

    #[test]
    pub fn test_proposal_record_with_votes_for() {
        let record = ProposalRecord {
            epoch: 1,
            height: 1024,
            miner_address_id: AddressID::miner(500),
            slots: [1, 2, 3],
            tally: [10, 5, 7],
        };
        assert_eq!(record.with_votes([1, 2, 3]).tally, [11, 6, 8]);
    }

    #[test]
    pub fn test_proposal_record_with_votes_against() {
        let record = ProposalRecord {
            epoch: 1,
            height: 1024,
            miner_address_id: AddressID::miner(500),
            slots: [1, 2, 3],
            tally: [10, 5, 7],
        };
        assert_eq!(record.with_votes([10, 20, 30]), record);
    }

    #[test]
    pub fn test_proposal_record_withdraw_votes_but_no_proposal() {
        let record = ProposalRecord {
            epoch: 1,
            height: 1024,
            miner_address_id: AddressID::miner(500),
            slots: [0, 0, 0],
            tally: [0, 0, 0],
        };
        assert_eq!(record.withdraw_votes([0, 1, 0]), record);
    }

    #[test]
    pub fn test_proposal_record_withdraw_votes_for() {
        let record = ProposalRecord {
            epoch: 1,
            height: 1024,
            miner_address_id: AddressID::miner(500),
            slots: [1, 2, 3],
            tally: [10, 5, 7],
        };
        assert_eq!(record.withdraw_votes([1, 2, 3]).tally, [9, 4, 6]);
    }

    #[test]
    pub fn test_proposal_record_withdraw_votes_against() {
        let record = ProposalRecord {
            epoch: 1,
            height: 1024,
            miner_address_id: AddressID::miner(500),
            slots: [1, 2, 3],
            tally: [10, 5, 7],
        };
        assert_eq!(record.withdraw_votes([10, 20, 30]), record);
    }

    #[test]
    pub fn test_votes_record_pack() {
        let record = VotesRecord {
            height: 100,
            slot1: 1,
            slot2: 2,
            slot3: 3,
        };
        assert_eq!(record.pack(), [1i8, 2i8, 3i8]);
    }

    #[test]
    pub fn test_proposal_from_record_new() {
        let record = ProposalRecord {
            epoch: 100,
            height: 102400,
            miner_address_id: AddressID::miner(500),
            slots: [1, 0, 0],
            tally: [0, 0, 0],
        };
        let proposal = Proposal::from(Some(record));
        assert!(matches!(proposal, Proposal::New(_)));
    }

    #[test]
    pub fn test_proposal_from_record_tally() {
        let record = ProposalRecord {
            epoch: 100,
            height: 102405,
            miner_address_id: AddressID::miner(500),
            slots: [1, 2, 0],
            tally: [3, 6, 0],
        };
        let proposal = Proposal::from(Some(record));
        assert!(matches!(proposal, Proposal::Tally(_)));
    }

    #[test]
    pub fn test_proposal_from_record_empty() {
        let proposal = Proposal::from(None);
        assert!(matches!(proposal, Proposal::Empty));
    }
}

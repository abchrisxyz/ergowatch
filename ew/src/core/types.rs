use super::ergo;
use super::node;
pub use super::node::models::Asset;

pub type Address = String;
pub type AddressID = i64;
pub type BoxID = Digest32;
pub type Digest32 = String;
pub type ErgoTree = String;
pub type HeaderID = Digest32;
pub type Height = i32;
pub type Registers = serde_json::Value;
pub type Timestamp = i64;
pub type TokenID = Digest32;
pub type TransactionID = Digest32;
pub type Version = u8;
pub type Value = i64;
pub type Votes = [i8; 3];
pub type NanoERG = i64;

const ZERO_HEADER: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug)]
pub struct CoreData {
    pub block: Block,
}

/// Pre-processed block data
#[derive(Debug)]
pub struct Block {
    pub header: Header,
    pub transactions: Vec<Transaction>,
    // pub transactions_size: i32,
    pub extension: node::models::Extension,
    pub ad_proofs: node::models::ADProofs,
    pub size: i32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Head {
    pub height: Height,
    pub header_id: HeaderID,
}

impl Head {
    pub fn new(height: i32, header_id: HeaderID) -> Self {
        Self { height, header_id }
    }
    /// A head representing blank state, before inclusion of genesis blocks.
    pub fn initial() -> Self {
        Self {
            height: -1,
            header_id: String::from(""),
        }
    }
    /// A head representing state right after inclusion of genesis blocks.
    pub fn genesis() -> Self {
        Self {
            height: 0,
            header_id: String::from(ZERO_HEADER),
        }
    }

    pub fn is_initial(&self) -> bool {
        self.height == -1 && self.header_id == ""
    }

    pub fn is_genesis(&self) -> bool {
        self.height == 0 && self.header_id == ZERO_HEADER
    }
}

#[derive(Debug)]
pub struct Header {
    pub extension_id: String,
    pub difficulty: String,
    pub votes: Votes,
    pub timestamp: Timestamp,
    pub size: i32,
    pub state_root: String,
    pub height: Height,
    pub n_bits: i64,
    pub version: Version,
    pub id: String,
    pub ad_proofs_root: String,
    pub transactions_root: String,
    pub extension_hash: String,
    pub pow_solutions: node::models::POWSolutions,
    pub ad_proofs_id: String,
    pub transactions_id: String,
    pub parent_id: HeaderID,
}

impl From<node::models::Header> for Header {
    fn from(header: node::models::Header) -> Self {
        Self {
            extension_id: header.extension_id,
            difficulty: header.difficulty,
            votes: ergo::votes::from_str(&header.votes),
            timestamp: header.timestamp,
            size: header.size,
            state_root: header.state_root,
            height: header.height,
            n_bits: header.n_bits,
            version: header.version,
            id: header.id,
            ad_proofs_root: header.ad_proofs_root,
            transactions_root: header.transactions_root,
            extension_hash: header.extension_hash,
            pow_solutions: header.pow_solutions,
            ad_proofs_id: header.ad_proofs_id,
            transactions_id: header.transactions_id,
            parent_id: header.parent_id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: Digest32,
    pub index: i32,
    pub outputs: Vec<Output>,
    pub inputs: Vec<Input>,
    pub data_inputs: Vec<Input>,
}

/// Mutually exclusive address attributes
#[derive(Debug)]
pub enum AddressType {
    P2PK,
    MINER,
    OTHER,
}

#[derive(Debug, Clone)]
pub struct Output {
    pub box_id: BoxID,
    pub creation_height: Height,
    pub address_id: AddressID,
    pub index: i32,
    pub value: i64,
    pub additional_registers: serde_json::Value,
    pub assets: Vec<node::models::Asset>,
    pub size: i32,
}

impl Output {
    pub fn from_node_output(output: node::models::Output, address_id: AddressID) -> Self {
        let size = match ergo::boxes::calc_box_size(&output) {
            Some(s) => s,
            None => 0,
        };
        Self {
            box_id: output.box_id,
            creation_height: output.creation_height,
            address_id: address_id,
            index: output.index,
            value: output.value,
            additional_registers: output.additional_registers,
            assets: output.assets,
            size: size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Input {
    pub box_id: BoxID,
    pub address_id: AddressID,
    pub index: i32,
    pub value: i64,
    pub additional_registers: serde_json::Value,
    pub assets: Vec<node::models::Asset>,
    pub size: i32,
    pub creation_height: Height,
    pub creation_timestamp: Timestamp,
}

#[derive(Debug)]
pub struct Register {
    pub id: i16,
    pub stype: String,
    pub serialized_value: String,
    pub rendered_value: String,
}

fn parse_additional_registers(regs: &serde_json::Value) -> [Option<Register>; 6] {
    match regs {
        serde_json::Value::Null => [None, None, None, None, None, None],
        serde_json::Value::Object(map) => [
            match map.get("R4") {
                Some(v) => decode_register(&v, 4),
                None => None,
            },
            match map.get("R5") {
                Some(v) => decode_register(&v, 5),
                None => None,
            },
            match map.get("R6") {
                Some(v) => decode_register(&v, 6),
                None => None,
            },
            match map.get("R7") {
                Some(v) => decode_register(&v, 7),
                None => None,
            },
            match map.get("R8") {
                Some(v) => decode_register(&v, 8),
                None => None,
            },
            match map.get("R9") {
                Some(v) => decode_register(&v, 9),
                None => None,
            },
        ],
        _ => {
            panic!("Non map object for additional registers: {:?}", &regs);
        }
    }
}

fn decode_register(value: &serde_json::Value, id: i16) -> Option<Register> {
    if let serde_json::Value::String(s) = value {
        let rendered_register = ergo::register::render_register_value(&s);
        return Some(Register {
            id: id,
            stype: rendered_register.value_type,
            serialized_value: s.to_string(),
            rendered_value: rendered_register.value,
        });
    }
    panic!("Non string value in register: {}", value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::node::models::Asset;
    use rand::distributions::Alphanumeric;
    use rand::distributions::DistString;

    fn random_digest32() -> Digest32 {
        Alphanumeric.sample_string(&mut rand::thread_rng(), 64)
    }

    impl Transaction {
        pub fn dummy() -> Self {
            Self {
                id: random_digest32(),
                index: 0,
                outputs: vec![],
                inputs: vec![],
                data_inputs: vec![],
            }
        }

        /// Returns tx with appended input. Sets the input's index.
        pub fn add_input(&self, input: Input) -> Self {
            let mut tx = self.clone();
            let idx = self.inputs.len() as i32;
            tx.inputs.push(input.index(idx));
            tx
        }

        /// Returns tx with appended output. Sets the output's index.
        pub fn add_output(&self, output: Output) -> Self {
            let mut tx = self.clone();
            let idx = self.outputs.len() as i32;
            tx.outputs.push(output.index(idx));
            tx
        }
    }

    impl Output {
        pub fn dummy() -> Self {
            Self {
                box_id: Alphanumeric.sample_string(&mut rand::thread_rng(), 64),
                creation_height: 0,
                address_id: 0,
                index: 0,
                value: 1000000000,
                additional_registers: "{}".into(),
                assets: vec![],
                size: 100,
            }
        }

        /// Returns output with modified value
        pub fn value(&self, value: NanoERG) -> Self {
            let mut output = self.clone();
            output.value = value;
            output
        }

        /// Returns output with modified address id
        pub fn address_id(&self, address_id: AddressID) -> Self {
            let mut output = self.clone();
            output.address_id = address_id;
            output
        }

        /// Returns output with modified index
        pub fn index(&self, index: i32) -> Self {
            let mut output = self.clone();
            output.index = index;
            output
        }

        /// Returns output with asset added
        pub fn add_asset(&self, token_id: &str, amount: i64) -> Self {
            let mut output = self.clone();
            let asset = Asset {
                token_id: token_id.into(),
                amount,
            };
            output.assets.push(asset);
            output
        }
    }

    impl Input {
        pub fn dummy() -> Self {
            Self {
                box_id: Alphanumeric.sample_string(&mut rand::thread_rng(), 64),
                creation_height: 0,
                address_id: 0,
                index: 0,
                value: 1000000000,
                additional_registers: "{}".into(),
                assets: vec![],
                size: 100,
                creation_timestamp: 1683634223508,
            }
        }

        /// Returns input with modified value
        pub fn value(&self, value: NanoERG) -> Self {
            let mut input = self.clone();
            input.value = value;
            input
        }

        /// Returns input with modified address id
        pub fn address_id(&self, address_id: AddressID) -> Self {
            let mut input = self.clone();
            input.address_id = address_id;
            input
        }

        /// Returns input with modified index
        pub fn index(&self, index: i32) -> Self {
            let mut input = self.clone();
            input.index = index;
            input
        }

        /// Returns input with asset added
        pub fn add_asset(&self, token_id: &str, amount: i64) -> Self {
            let mut input = self.clone();
            let asset = Asset {
                token_id: token_id.into(),
                amount,
            };
            input.assets.push(asset);
            input
        }
    }

    #[test]
    fn test_output_helpers() {
        let output = Output::dummy()
            .index(3)
            .address_id(123)
            .value(12345)
            .add_asset("some-token", 420);
        assert_eq!(output.index, 3);
        assert_eq!(output.address_id, 123);
        assert_eq!(output.value, 12345);
        assert_eq!(output.assets[0].token_id, String::from("some-token"));
        assert_eq!(output.assets[0].amount, 420);
    }

    #[test]
    fn test_input_helpers() {
        let input = Input::dummy()
            .index(3)
            .address_id(123)
            .value(12345)
            .add_asset("some-token", 420);
        assert_eq!(input.index, 3);
        assert_eq!(input.address_id, 123);
        assert_eq!(input.value, 12345);
        assert_eq!(input.assets[0].token_id, String::from("some-token"));
        assert_eq!(input.assets[0].amount, 420);
    }

    #[test]
    fn test_transaction_helpers() {
        let tx = Transaction::dummy();
        // inputs
        assert!(tx.inputs.is_empty());
        let tx = tx.add_input(Input::dummy().index(5));
        assert!(tx.inputs.len() == 1);
        assert!(tx.inputs[0].index == 0);
        // outputs
        assert!(tx.outputs.is_empty());
        let tx = tx.add_output(Output::dummy().index(5));
        assert!(tx.outputs.len() == 1);
        assert!(tx.outputs[0].index == 0);
    }
}

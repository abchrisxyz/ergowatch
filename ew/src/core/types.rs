use postgres_types::FromSql;
use postgres_types::ToSql;
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use tokio_postgres::types::private::BytesMut;

use crate::constants::GENESIS_TIMESTAMP;
use crate::constants::ZERO_HEADER;

use super::ergo;
use super::node;

pub type Address = String;
pub type AssetID = i64;
pub type BoxID = Digest32;
pub type Digest32 = String;
pub type ErgoTree = String;
pub type HeaderID = Digest32;
pub type Height = i32;
pub type Timestamp = i64;
pub type TokenID = Digest32;
pub type TransactionID = Digest32;
pub type Version = u8;
pub type Value = i64;
pub type Votes = [i8; 3];
pub type NanoERG = i64;

#[derive(Debug)]
pub struct CoreData {
    pub block: Block,
}

/// Pre-processed block data
#[derive(Debug)]
#[cfg_attr(feature = "test-utilities", derive(Clone))]
pub struct Block {
    pub header: BlockHeader,
    pub transactions: Vec<Transaction>,
    // pub transactions_size: i32,
    pub extension: node::models::Extension,
    // pub ad_proofs: node::models::ADProofs,
    pub size: i32,
}

impl Block {
    /// Get a collection of all address id's involved as input or output
    /// in a block's transactions.
    ///
    /// Ignores data-inputs.
    pub fn transacting_addresses(&self) -> Vec<AddressID> {
        let mut address_ids: HashSet<AddressID> = HashSet::new();
        for tx in &self.transactions {
            for input in &tx.inputs {
                address_ids.insert(input.address_id);
            }
            for output in &tx.outputs {
                address_ids.insert(output.address_id);
            }
        }
        address_ids.into_iter().collect()
    }

    /// Create a fake block holding genesis data.
    ///
    /// Avoids having to use a dedicated type for genesis only.
    pub fn from_genesis_boxes(boxes: Vec<BoxData>) -> Block {
        Self {
            header: BlockHeader {
                extension_id: "".to_owned(),
                difficulty: "1199990374400".to_owned(),
                votes: [0, 0, 0],
                timestamp: crate::constants::GENESIS_TIMESTAMP,
                size: 0,
                state_root: "".to_owned(),
                height: 0,
                n_bits: 0,
                version: 0,
                id: ZERO_HEADER.to_owned(),
                ad_proofs_root: "".to_owned(),
                transactions_root: "".to_owned(),
                extension_hash: "".to_owned(),
                pow_solutions: node::models::POWSolutions {
                    pk: "".to_owned(),
                    w: "".to_owned(),
                    n: "".to_owned(),
                    d: 0f64,
                },
                ad_proofs_id: "".to_owned(),
                transactions_id: "".to_owned(),
                parent_id: "".to_owned(),
            },
            transactions: vec![
                // Single fake transaction outputting all genesis boxes
                Transaction {
                    id: "".to_owned(),
                    index: 0,
                    outputs: boxes,
                    inputs: vec![],
                    data_inputs: vec![],
                },
            ],
            extension: node::models::Extension {
                header_id: ZERO_HEADER.to_owned(),
                digest: "".to_owned(),
                fields: vec![],
            },
            size: 0,
        }
    }
}

/// A short version of block headers
#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub height: Height,
    pub timestamp: Timestamp,
    pub header_id: HeaderID,
    pub parent_id: HeaderID,
}

impl Header {
    /// A header representing blank state, before inclusion of genesis blocks.
    pub fn initial() -> Self {
        Self {
            height: -1,
            timestamp: 0,
            header_id: String::from(""),
            parent_id: String::from(""),
        }
    }
    /// A head representing state right after inclusion of genesis blocks.
    pub fn genesis() -> Self {
        Self {
            height: 0,
            timestamp: GENESIS_TIMESTAMP,
            header_id: String::from(ZERO_HEADER),
            parent_id: String::from(""),
        }
    }

    pub fn is_initial(&self) -> bool {
        self.height == -1 && self.header_id == ""
    }

    pub fn is_genesis(&self) -> bool {
        self.height == 0 && self.header_id == ZERO_HEADER
    }
}

#[cfg(feature = "test-utilities")]
impl From<&BlockHeader> for Header {
    fn from(value: &BlockHeader) -> Self {
        Self {
            height: value.height,
            timestamp: value.timestamp,
            header_id: value.id.clone(),
            parent_id: value.parent_id.clone(),
        }
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "test-utilities", derive(Clone))]
pub struct BlockHeader {
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

impl From<node::models::Header> for BlockHeader {
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
    pub outputs: Vec<BoxData>,
    pub inputs: Vec<BoxData>,
    pub data_inputs: Vec<BoxData>,
}

impl Transaction {
    /// Return non-zero balance diffs by AddressID
    pub fn non_zero_diffs(&self) -> HashMap<AddressID, NanoERG> {
        let mut map = HashMap::new();
        for input in &self.inputs {
            match map.entry(input.address_id) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() -= input.value;
                }
                Entry::Vacant(e) => {
                    e.insert(-input.value);
                }
            }
        }
        for output in &self.outputs {
            match map.entry(output.address_id) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() += output.value;
                }
                Entry::Vacant(e) => {
                    e.insert(output.value);
                }
            }
        }
        map.retain(|_, v| v != &0);
        map
    }
}

/// Mutually exclusive address attributes
///
/// P2PK: pay to private key addresses
/// MINER: mining contracts
/// OTHER: other pay to script / script-hash addresses
#[derive(Debug, Clone, PartialEq, ToSql, FromSql)]
#[postgres(name = "address_type")]
pub enum AddressType {
    /// Pay to private key
    P2PK,
    /// Mining contract
    Miner,
    /// Other (non-mining) P2S(H) contracts
    Other,
}

/// Return the AddressType for a given `address`.
impl AddressType {
    /// Derive the AddressType for a given `address`.
    pub fn derive(address: &str) -> Self {
        if address.starts_with('9') && address.len() == 51 {
            return Self::P2PK;
        } else if address.starts_with("88dhgzEuTX") {
            // Ideally we'd use the ergo tree template hash here.
            // So far, this explorer query:
            //      select count(*)
            //      from node_outputs
            //      where ergo_tree_template_hash = '961e872f7ab750cb77ad75ea8a32d0ea3472bd0c230de09329b802801b3d1817'
            // 	    and address not ilike '88dhgzEuTX%'
            // has no matches, so '88dhgzEuTX' should be safe enough to use to id miner contracts.
            return Self::Miner;
        }
        Self::Other
    }
}

/// Unique ID representing a single address.
///
/// First (i.e. least significant) digit represents address type:
///
/// * ID's ending in 1: P2PK
/// * ID's ending in 2: mining contracts
/// * ID's ending in 3: other
///
/// Other digits represent a continuous sequence across all ID's.
/// This means there cannot be ID's differing with the last digit
/// only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AddressID(pub i64);

impl AddressID {
    const P2PK: i64 = 1;
    const MINER: i64 = 2;
    const OTHER: i64 = 3;

    /// New AddressID from sequence index and address.
    ///
    /// Derives the type from the provided `address`.
    ///
    /// * `n`: index of address in address sequence
    /// * `address`: actual address
    pub fn new(n: i64, address: &str) -> Self {
        Self(match AddressType::derive(address) {
            AddressType::P2PK => n * 10 + Self::P2PK,
            AddressType::Miner => n * 10 + Self::MINER,
            AddressType::Other => n * 10 + Self::OTHER,
        })
    }

    pub fn address_type(&self) -> AddressType {
        match self.0 % 10 {
            1 => AddressType::P2PK,
            2 => AddressType::Miner,
            3 => AddressType::Other,
            _ => panic!("Unknown address type encoded in address id"),
        }
    }

    /// Returns the address sequence position.
    pub fn sequence_position(&self) -> i64 {
        self.0 / 10
    }

    pub fn zero() -> Self {
        Self(0)
    }

    pub fn is_miner(&self) -> bool {
        self.address_type() == AddressType::Miner
    }

    #[cfg(feature = "test-utilities")]
    /// Convenience function to create a p2pk AddressID from given sequence position `n`
    pub fn p2pk(n: i64) -> Self {
        Self(n * 10 + Self::P2PK)
    }

    #[cfg(feature = "test-utilities")]
    /// Convenience function to create a miner AddressID from given sequence position `n`
    pub fn miner(n: i64) -> Self {
        Self(n * 10 + Self::MINER)
    }

    #[cfg(feature = "test-utilities")]
    /// Convenience function to create an AddressID with type "other" from given sequence position `n`
    pub fn other(n: i64) -> Self {
        Self(n * 10 + Self::OTHER)
    }
}

impl<'a> FromSql<'a> for AddressID {
    fn from_sql(
        ty: &postgres_types::Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        match i64::from_sql(ty, raw) {
            Ok(value) => Ok(AddressID(value)),
            Err(err) => Err(err),
        }
    }

    fn accepts(ty: &postgres_types::Type) -> bool {
        <i64 as FromSql>::accepts(ty)
    }
}

impl ToSql for AddressID {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        ToSql::to_sql(&self.0, ty, out)
    }

    fn accepts(ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        <i64 as ToSql>::accepts(ty)
    }

    postgres_types::to_sql_checked!();
}

/// In/Output agnostic box data.
#[derive(Debug, Clone)]
pub struct BoxData {
    pub box_id: BoxID,
    pub creation_height: Height,
    pub address_id: AddressID,
    pub value: i64,
    pub additional_registers: Registers,
    pub assets: Vec<Asset>,
    pub size: i32,
    /// Timestamp of the block this box was created in. Not necessarily
    /// corresponding to `creation_height`.
    pub output_timestamp: Timestamp,
}

#[derive(Debug, Clone, ToSql, FromSql)]
#[postgres(name = "asset")]
pub struct Asset {
    pub asset_id: AssetID,
    pub amount: Value,
}

/// Wraps registers json and provides parsing methods
#[derive(Debug, Clone, Serialize)]
pub struct Registers(serde_json::Value);

impl Registers {
    pub fn new(json: serde_json::Value) -> Self {
        Self(json)
    }

    /// Check if R4 is set
    pub fn has_r4(&self) -> bool {
        self.check_register("R4")
    }

    /// Check if R5 is set
    pub fn has_r5(&self) -> bool {
        self.check_register("R5")
    }

    /// Check if R6 is set
    pub fn has_r6(&self) -> bool {
        self.check_register("R6")
    }

    /// Rendered R4 register
    pub fn r4(&self) -> Option<Register> {
        self.render_register("R4", 4)
    }

    fn check_register(&self, key: &str) -> bool {
        match &self.0 {
            serde_json::Value::Null => false,
            serde_json::Value::Object(map) => map.get(key).is_some(),
            _ => {
                panic!("Non map object for additional registers: {:?}", &self.0);
            }
        }
    }

    fn render_register(&self, key: &str, id: i16) -> Option<Register> {
        match &self.0 {
            serde_json::Value::Null => None,
            serde_json::Value::Object(map) => match map.get(key) {
                Some(v) => decode_register(v, id),
                None => None,
            },
            _ => {
                panic!("Non map object for additional registers: {:?}", &self.0);
            }
        }
    }
}

#[derive(Debug)]
pub struct Register {
    pub id: i16,
    pub stype: String,
    pub serialized_value: String,
    pub rendered_value: String,
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

#[cfg(feature = "test-utilities")]
pub mod testutils {
    pub use super::*;
    use crate::core::node::models::Extension;
    use crate::core::node::models::ExtensionField;
    use crate::core::node::models::POWSolutions;
    use rand::distributions::Alphanumeric;
    use rand::distributions::DistString;

    pub fn random_digest32() -> Digest32 {
        Alphanumeric.sample_string(&mut rand::thread_rng(), 64)
    }

    impl Block {
        /// (test-util) Creates a dummy block.
        pub fn dummy() -> Self {
            Block {
                header: BlockHeader::dummy(),
                transactions: vec![],
                extension: Extension {
                    header_id: random_digest32(),
                    digest: random_digest32(),
                    fields: vec![],
                },
                // ad_proofs: ADProofs {
                //     header_id: random_digest32(),
                //     proof_bytes: "".to_string(),
                //     digest: random_digest32(),
                //     size: 7112,
                // },
                size: 8488,
            }
        }

        /// Returns a dummy block representing a child of `other`.
        ///
        /// Child block will have incremented height and parent_id
        /// set to `other` header_id.
        pub fn child_of(other: &Self) -> Self {
            Self::dummy()
                .height(other.header.height + 1)
                .parent_id(&other.header.id)
        }

        /// (test-util) Returns block with modified header height.
        pub fn height(&self, height: Height) -> Self {
            let mut block = self.clone();
            block.header.height = height;
            block
        }

        /// (test-util) Returns block with modified header timestamp.
        pub fn timestamp(&self, t: Timestamp) -> Self {
            let mut block = self.clone();
            block.header.timestamp = t;
            block
        }

        /// (test-util) Returns block with modified header parent_id.
        pub fn parent_id(&self, parent_id: &str) -> Self {
            let mut block = self.clone();
            block.header.parent_id = parent_id.to_owned();
            block
        }

        /// (test-util) Returns block with modified header votes.
        pub fn votes(&self, votes: [i8; 3]) -> Self {
            let mut block = self.clone();
            block.header.votes = votes;
            block
        }

        /// (test-util) Returns block with appended transaction.
        pub fn add_tx(&self, tx: Transaction) -> Self {
            let mut block = self.clone();
            block.transactions.push(tx);
            block
        }

        /// (test-util) Returns block with appended extension field.
        pub fn add_extension_field(&self, key: &str, value: &str) -> Self {
            let mut block = self.clone();
            block.extension.fields.push(ExtensionField {
                key: key.to_owned(),
                value: value.to_owned(),
            });
            block
        }
    }

    impl BlockHeader {
        /// (test-util) Dummy header
        pub fn dummy() -> Self {
            BlockHeader {
                extension_id: random_digest32(),
                difficulty: "2187147670978560".to_string(),
                votes: [0, 0, 0],
                timestamp: 1634511451404,
                size: 221,
                state_root: random_digest32(),
                height: 600_000,
                n_bits: 117949747,
                version: 2,
                id: random_digest32(),
                ad_proofs_root: "".to_string(),
                transactions_root: "".to_string(),
                extension_hash: "".to_string(),
                pow_solutions: POWSolutions {
                    d: 0.,
                    n: "6d33ee4161329eec".to_string(),
                    pk: "029ed28cae37942d25d5cc5f0ade4b1b2e03e18b05c4f3233018bf67c817709f93"
                        .to_string(),
                    w: "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
                        .to_string(),
                },
                ad_proofs_id: "".to_string(),
                transactions_id: "".to_string(),
                parent_id: random_digest32(),
            }
        }
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

        /// Returns tx with appended input.
        pub fn add_input(&self, input: BoxData) -> Self {
            let mut tx = self.clone();
            tx.inputs.push(input);
            tx
        }

        /// Returns tx with appended data-input.
        pub fn add_data_input(&self, input: BoxData) -> Self {
            let mut tx = self.clone();
            tx.data_inputs.push(input);
            tx
        }

        /// Returns tx with appended output.
        pub fn add_output(&self, output: BoxData) -> Self {
            let mut tx = self.clone();
            tx.outputs.push(output);
            tx
        }
    }

    impl BoxData {
        /// (test-util) Creates a BoxData with dummy/random data.
        pub fn dummy() -> Self {
            Self {
                box_id: Alphanumeric.sample_string(&mut rand::thread_rng(), 64),
                creation_height: 0,
                address_id: AddressID(1),
                value: 1000000000,
                size: 100,
                assets: vec![],
                additional_registers: Registers::dummy(),
                output_timestamp: 1683634223508,
            }
        }

        /// (test-util) Returns box with modified creation height
        pub fn creation_height(&self, height: Height) -> Self {
            let mut bx = self.clone();
            bx.creation_height = height;
            bx
        }

        /// (test-util) Returns box with modified timestamp
        pub fn timestamp(&self, timestamp: Timestamp) -> Self {
            let mut bx = self.clone();
            bx.output_timestamp = timestamp;
            bx
        }

        /// (test-util) Returns box with modified value
        pub fn value(&self, value: NanoERG) -> Self {
            let mut bx = self.clone();
            bx.value = value;
            bx
        }

        /// (test-util) Returns box with modified address id
        pub fn address_id(&self, address_id: AddressID) -> Self {
            let mut input = self.clone();
            input.address_id = address_id;
            input
        }

        /// (test-util) Returns box with asset added
        pub fn add_asset(&self, asset_id: AssetID, amount: i64) -> Self {
            let mut bx = self.clone();
            let asset = Asset { asset_id, amount };
            bx.assets.push(asset);
            bx
        }

        /// (test-util)  Set serialized register value
        pub fn set_registers(&self, json: &str) -> Self {
            let mut bx = self.clone();
            bx.additional_registers = Registers::new(serde_json::from_str(json).unwrap());
            bx
        }
    }

    impl Registers {
        pub fn dummy() -> Self {
            Self("{}".into())
        }
    }

    impl AddressID {
        pub fn dummy(id: i64) -> Self {
            Self(id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_box_data_helpers() {
        let output = BoxData::dummy()
            .address_id(AddressID::dummy(123))
            .creation_height(601)
            .timestamp(1683634123456)
            .value(12345)
            .add_asset(5, 420)
            .set_registers(r#"{"R4": "05baafd2a302"}"#);
        assert_eq!(output.address_id, AddressID::dummy(123));
        assert_eq!(output.creation_height, 601);
        assert_eq!(output.output_timestamp, 1683634123456);
        assert_eq!(output.value, 12345);
        assert_eq!(output.assets[0].asset_id, 5);
        assert_eq!(output.assets[0].amount, 420);
        assert_eq!(
            output.additional_registers.r4().expect("R4").rendered_value,
            "305810397"
        );
    }

    #[test]
    fn test_transaction_helpers() {
        let tx = Transaction::dummy();
        // inputs
        assert!(tx.inputs.is_empty());
        let tx = tx.add_input(BoxData::dummy().address_id(AddressID::dummy(123)));
        assert_eq!(tx.inputs.len(), 1);
        assert_eq!(tx.inputs[0].address_id, AddressID::dummy(123));

        // data-inputs
        assert!(tx.data_inputs.is_empty());
        let tx = tx.add_data_input(BoxData::dummy().address_id(AddressID::dummy(234)));
        assert_eq!(tx.data_inputs.len(), 1);
        assert_eq!(tx.data_inputs[0].address_id, AddressID::dummy(234));

        // outputs
        assert!(tx.outputs.is_empty());
        let tx = tx.add_output(BoxData::dummy().address_id(AddressID::dummy(456)));
        assert_eq!(tx.outputs.len(), 1);
        assert_eq!(tx.outputs[0].address_id, AddressID::dummy(456));
    }

    #[test]
    fn test_address_id_type() {
        assert_eq!(AddressID(101).address_type(), AddressType::P2PK);
        assert_eq!(AddressID(102).address_type(), AddressType::Miner);
        assert_eq!(AddressID(103).address_type(), AddressType::Other);
    }

    #[test]
    fn test_address_id_sequence_position() {
        assert_eq!(AddressID(991).sequence_position(), 99);
    }

    #[test]
    fn test_block_addresses() {
        let block = Block::dummy()
            .add_tx(
                Transaction::dummy()
                    .add_input(BoxData::dummy().address_id(AddressID::dummy(123)))
                    // Data inputs should be ignored
                    .add_data_input(BoxData::dummy().address_id(AddressID::dummy(100)))
                    // but only if they're not present as input/output
                    .add_data_input(BoxData::dummy().address_id(AddressID::dummy(123)))
                    .add_output(BoxData::dummy().address_id(AddressID::dummy(456))),
            )
            .add_tx(
                Transaction::dummy()
                    .add_input(BoxData::dummy().address_id(AddressID::dummy(456)))
                    .add_output(BoxData::dummy().address_id(AddressID::dummy(789))),
            );
        let address_ids = block.transacting_addresses();
        assert_eq!(address_ids.len(), 3);
        assert!(address_ids.contains(&AddressID::dummy(123)));
        assert!(address_ids.contains(&AddressID::dummy(456)));
        assert!(address_ids.contains(&AddressID::dummy(789)));
    }

    #[test]
    fn test_transaction_non_zero_diffs() {
        let addr_a = AddressID(5);
        let addr_b = AddressID(6);
        let addr_c = AddressID(7);
        let tx = Transaction::dummy()
            .add_input(BoxData::dummy().address_id(addr_a).value(1000))
            .add_input(BoxData::dummy().address_id(addr_b).value(2000))
            .add_input(BoxData::dummy().address_id(addr_c).value(3000))
            .add_input(BoxData::dummy().address_id(addr_a).value(4000))
            .add_output(BoxData::dummy().address_id(addr_a).value(7000))
            .add_output(BoxData::dummy().address_id(addr_c).value(1500))
            .add_output(BoxData::dummy().address_id(addr_c).value(1500));
        let diffs = tx.non_zero_diffs();
        assert_eq!(diffs.len(), 2);
        assert_eq!(diffs[&addr_a], 2000);
        assert_eq!(diffs[&addr_b], -2000);
    }
}

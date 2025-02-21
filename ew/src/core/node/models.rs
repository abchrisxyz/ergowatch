use serde::Deserialize;

use crate::core::types::BoxID;
use crate::core::types::Digest32;
use crate::core::types::ErgoTree;
use crate::core::types::HeaderID;
use crate::core::types::Height;
use crate::core::types::Timestamp;
use crate::core::types::TokenID;
use crate::core::types::TransactionID;
use crate::core::types::Value;
use crate::core::types::Version;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfo {
    pub full_height: Height,
    pub best_full_header_id: HeaderID,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub header: Header,
    pub block_transactions: BlockTransactions,
    pub extension: Extension,
    // pub ad_proofs: ADProofs,
    pub size: i32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Header {
    pub extension_id: Digest32,
    pub difficulty: String,
    pub votes: String,
    pub timestamp: Timestamp,
    pub size: i32,
    pub state_root: String,
    pub height: Height,
    pub n_bits: i64,
    pub version: Version,
    pub id: HeaderID,
    pub ad_proofs_root: Digest32,
    pub transactions_root: Digest32,
    pub extension_hash: Digest32,
    pub pow_solutions: POWSolutions,
    pub ad_proofs_id: Digest32,
    pub transactions_id: Digest32,
    pub parent_id: HeaderID,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "test-utilities", derive(Clone))]
pub struct POWSolutions {
    pub pk: String,
    pub w: String,
    pub n: String,
    pub d: f64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BlockTransactions {
    pub header_id: HeaderID,
    pub transactions: Vec<Transaction>,
    pub block_version: Version,
    pub size: i32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub id: TransactionID,
    pub inputs: Vec<Input>,
    pub data_inputs: Vec<DataInput>,
    pub outputs: Vec<Output>,
    pub size: i32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Input {
    pub box_id: BoxID,
    pub spending_proof: SpendingProof,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpendingProof {
    pub proof_bytes: String,
    // pub extension: ...
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DataInput {
    pub box_id: BoxID,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Output {
    pub box_id: BoxID,
    pub value: Value,
    pub ergo_tree: ErgoTree,
    pub assets: Vec<Asset>,
    pub creation_height: Height,
    pub additional_registers: serde_json::Value,
    pub transaction_id: TransactionID,
    pub index: i32,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    pub token_id: TokenID,
    pub amount: i64,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "test-utilities", derive(Clone))]
pub struct Extension {
    pub header_id: HeaderID,
    pub digest: Digest32,
    pub fields: Vec<ExtensionField>,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(feature = "test-utilities", derive(Clone))]
pub struct ExtensionField {
    pub key: String,
    pub value: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "test-utilities", derive(Clone))]
pub struct ADProofs {
    pub _header_id: HeaderID,
    pub _proof_bytes: String,
    pub _digest: Digest32,
    pub _size: i32,
}

#[cfg(test)]
impl Block {
    /// Consume Block and return Transaction at given `index`
    pub fn take_tx(self, index: usize) -> Transaction {
        self.block_transactions
            .transactions
            .into_iter()
            .nth(index)
            .unwrap()
    }
}

#[cfg(test)]
impl Transaction {
    /// Consume Transaction and return Output at given `index`
    pub fn take_output(self, index: usize) -> Output {
        self.outputs.into_iter().nth(index).unwrap()
    }
}

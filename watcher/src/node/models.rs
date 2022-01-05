use std::collections::HashMap;

use serde::Deserialize;

pub type BoxID = Digest32;
pub type Digest32 = String;
pub type HeaderID = Digest32;
pub type Height = u32;
pub type Timestamp = u64;
pub type TokenID = Digest32;
pub type TransactionID = Digest32;
pub type Version = u8;
pub type Value = u64;
pub type Registers = HashMap<String, String>;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct NodeInfo {
    pub full_height: Height,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub header: Header,
    pub block_transactions: BlockTransactions,
    // pub extension: Extension,
    // pub ad_proofs: ADProofs,
    pub size: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Header {
    // pub extension_id: Digest32,
    // pub difficulty: String,
    pub votes: String,
    pub timestamp: Timestamp,
    pub size: u32,
    // pub state_root: Digest32,
    pub height: Height,
    // pub n_bits: u32,
    // pub version: Version,
    pub id: HeaderID,
    // pub ad_proofs_root: Digest32,
    // pub transactions_root: Digest32,
    // pub extension_hash: Digest32,
    // pub pow_solutions: ...,
    // pub ad_proofs_id: Digest32,
    pub transactions_id: Digest32,
    pub parent_id: HeaderID,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BlockTransactions {
    pub header_id: HeaderID,
    pub transactions: Vec<Transaction>,
    pub block_version: Version,
    pub size: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub id: TransactionID,
    pub inputs: Vec<Input>,
    pub data_inputs: Vec<DataInput>,
    pub outputs: Vec<Output>,
    pub size: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Input {
    pub box_id: BoxID,
    // pub spending_proof: SpendingProof,
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
    // pub ergo_tree: ErgoTree,
    pub assets: Vec<Asset>,
    pub creation_height: Height,
    pub additional_registers: Registers,
    pub transaction_id: TransactionID,
    pub index: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    pub token_id: TokenID,
    pub amount: u64,
}

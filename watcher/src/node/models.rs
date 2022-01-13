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
    // pub transactions_id: Digest32,
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

#[cfg(test)]
pub mod testing {
    use super::Asset;
    use super::Block;
    use super::BlockTransactions;
    use super::DataInput;
    use super::Header;
    use super::Input;
    use super::Output;
    use super::Transaction;
    use std::collections::HashMap;

    pub fn block_600k() -> Block {
        Block {
            header: Header {
                votes: String::from("000000"),
                timestamp: 1634511451404,
                size: 221,
                height: 600000,
                id: String::from(
                    "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4",
                ),
                parent_id: String::from(
                    "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1",
                ),
            },
            block_transactions: BlockTransactions {
                header_id: String::from(
                    "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4",
                ),
                transactions: vec![
                    Transaction {
                        id: String::from(
                            "4ac89169a2f83adb895b3d76735dbcfc63ad7940bddc2492d9ee4201299bf927",
                        ),
                        inputs: vec![Input {
                            box_id: String::from(
                                "eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f",
                            ),
                        }],
                        data_inputs: vec![],
                        outputs: vec![
                            Output {
                                box_id: String::from(
                                    "029bc1cb151aaef51c3678d2c74f3e82c9f4d197dd37e7a4eb73612f9da4f1f6",
                                ),
                                value: 52909132500000000,
                                assets: vec![],
                                creation_height: 600000,
                                additional_registers: HashMap::from([]),
                                transaction_id: String::from(
                                    "4ac89169a2f83adb895b3d76735dbcfc63ad7940bddc2492d9ee4201299bf927",
                                ),
                                index: 0,
                            },
                            Output {
                                box_id: String::from(
                                    "6cb8ffe391838b627cb893c9b2027aa2a03f3a20455dd11e5ac903c7e4179ace",
                                ),
                                value: 67500000000,
                                assets: vec![],
                                creation_height: 600000,
                                additional_registers: HashMap::from([]),
                                transaction_id: String::from(
                                    "4ac89169a2f83adb895b3d76735dbcfc63ad7940bddc2492d9ee4201299bf927",
                                ),
                                index: 1,
                            }
                        ],
                        size: 344,
                    },
                    Transaction {
                        id: String::from(
                            "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8",
                        ),
                        inputs: vec![
                            Input {
                                box_id: String::from("c739a3294d592377a131840d491bd2b66c27f51ae2c62c66be7bb41b248f321e"),
                            },
                            Input {
                                box_id: String::from("6ca2a9d63f2f08663c09d99126ec1be7b65ce2e8f34e283c4d5af78485b47c91"),
                            }
                        ],
                        data_inputs: vec![
                            DataInput {
                                box_id: String::from("98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"),
                            }
                        ],
                        outputs: vec![
                            Output {
                                box_id: String::from(
                                    "aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d",
                                ),
                                value: 1000000,
                                assets: vec![
                                    Asset {
                                        token_id: String::from("01e6498911823f4d36deaf49a964e883b2c4ae2a4530926f18b9c1411ab2a2c2"),
                                        amount: 1,
                                    }
                                ],
                                creation_height: 599998,
                                additional_registers: HashMap::from([
                                    (String::from("R4"), String::from("0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8")),
                                    (String::from("R5"), String::from("0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8")),
                                    (String::from("R6"), String::from("05a4c3edd9998877")),
                                ]),
                                transaction_id: String::from(
                                    "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8",
                                ),
                                index: 0,
                            },
                            Output {
                                box_id: String::from(
                                    "5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4",
                                ),
                                value: 1100000,
                                assets: vec![],
                                creation_height: 599998,
                                additional_registers: HashMap::from([]),
                                transaction_id: String::from(
                                    "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8",
                                ),
                                index: 1,
                            },
                            Output {
                                box_id: String::from(
                                    "22adc6d1fd18e81da0ab9fa47bc389c5948780c98906c0ea3d812eba4ef17a33",
                                ),
                                value: 2784172525,
                                assets: vec![],
                                creation_height: 599998,
                                additional_registers: HashMap::from([]),
                                transaction_id: String::from(
                                    "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8",
                                ),
                                index: 2,
                            }
                        ],
                        size: 674,
                    },
                    Transaction {
                        id: String::from(
                            "db3d79ab228b1b93bcb8cd742bacb0a4b49ad5fe67cc11b495482b8c541d3ae2",
                        ),
                        inputs: vec![
                            Input {
                                box_id: String::from("5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4"),
                            }
                        ],
                        data_inputs: vec![],
                        outputs: vec![
                            Output {
                                box_id: String::from(
                                    "98d0271b7a29d62b672d8dd002e38b8cfbfc8e4055a637422b3e9d59cd6ff86d",
                                ),
                                value: 1100000,
                                assets: vec![],
                                creation_height: 600000,
                                additional_registers: HashMap::from([]),
                                transaction_id: String::from(
                                    "db3d79ab228b1b93bcb8cd742bacb0a4b49ad5fe67cc11b495482b8c541d3ae2",
                                ),
                                index: 0,
                            },
                        ],
                        size: 100,
                    },
                ],
                block_version: 2,
                size: 1155,
            },
            size: 8486,
        }
    }
}

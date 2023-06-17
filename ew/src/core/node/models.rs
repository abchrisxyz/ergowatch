use serde::Deserialize;

use crate::core::types::BoxID;
use crate::core::types::Digest32;
use crate::core::types::ErgoTree;
use crate::core::types::HeaderID;
use crate::core::types::Height;
use crate::core::types::Timestamp;
use crate::core::types::TokenID;
use crate::core::types::TransactionID;
use crate::core::types::Version;
use crate::core::types::Value;
use crate::core::types::Registers;

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
    pub ad_proofs: ADProofs,
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
    pub additional_registers: Registers,
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
pub struct Extension {
    pub header_id: HeaderID,
    pub digest: Digest32,
    pub fields: Vec<ExtensionField>,
}

#[derive(Deserialize, Debug)]
pub struct ExtensionField {
    pub key: String,
    pub value: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ADProofs {
    pub header_id: HeaderID,
    pub proof_bytes: String,
    pub digest: Digest32,
    pub size: i32,
}

#[cfg(test)]
impl Block {
    /// Consume Block and return Transaction at given `index`
    pub fn take_tx(self, index: usize) -> Transaction {
        self.block_transactions.transactions.into_iter().nth(index).unwrap()
    }
}

#[cfg(test)]
impl Transaction {
    /// Consume Transaction and return Output at given `index`
    pub fn take_output(self, index: usize) -> Output {
        self.outputs.into_iter().nth(index).unwrap()
    }
}

#[cfg(test)]
pub mod testing {
    use super::ADProofs;
    use super::Asset;
    use super::Block;
    use super::BlockTransactions;
    use super::DataInput;
    use super::Extension;
    use super::ExtensionField;
    use super::Header;
    use super::Input;
    use super::Output;
    use super::POWSolutions;
    use super::SpendingProof;
    use super::Transaction;
    use serde_json;

    
    pub fn block_600k() -> Block {
        Block {
            header: Header {
                extension_id: String::from("cd5d15ce2a17d557ed1f87e5fc03a76de6e67e79dfbe41750b69c21409938454"),
                difficulty: String::from("185435213004800"),
                votes: String::from("000000"),
                timestamp: 1634511451404,
                size: 221,
                state_root: String::from("7183bac849bcf708af98e14877b32cdc1209fce849b0a18ed9abdd03625b112317"),
                height: 600000,
                n_bits: 117949747,
                version: 2,
                id: String::from(
                    "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4",
                ),
                ad_proofs_root: String::from("f391048a64b318f8a0e0b9380e94c546c0eaddabb6a10dd0daa48a04801b1a26"),
                transactions_root: String::from("dc4380b9797a1c2de3815f5e672ebae48205410098a3b511750cf69318ae72d5"),
                extension_hash: String::from("778c6259877d559672bcdf195173495bd296ec2f3600aa934b7181df28927e44"),
                pow_solutions: POWSolutions {
                    pk: String::from("029ed28cae37942d25d5cc5f0ade4b1b2e03e18b05c4f3233018bf67c817709f93"),
                    w: String::from("0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"),
                    n: String::from("6d33ee4161329eec"),
                    d: 0f64,
                },
                ad_proofs_id: String::from("fba99a30f50f60130c1f4fdaf38c6f33c0d5bab2bf33d47b6c582efb59016013"),
                transactions_id: String::from("275d347630fa69974104fa374883f57a964b91dcd0e9cda9cfab097c3434727c"),
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
                            spending_proof: SpendingProof { proof_bytes: String::from("") }
                        }],
                        data_inputs: vec![],
                        outputs: vec![
                            Output {
                                box_id: String::from(
                                    "029bc1cb151aaef51c3678d2c74f3e82c9f4d197dd37e7a4eb73612f9da4f1f6",
                                ),
                                value: 52909132500000000,
                                ergo_tree: String::from("101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f"),
                                assets: vec![],
                                creation_height: 600000,
                                additional_registers: serde_json::json!({}),
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
                                ergo_tree: String::from("100204a00b08cd029ed28cae37942d25d5cc5f0ade4b1b2e03e18b05c4f3233018bf67c817709f93ea02d192a39a8cc7a70173007301"),
                                assets: vec![],
                                creation_height: 600000,
                                additional_registers: serde_json::json!({}),
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
                                spending_proof: SpendingProof { proof_bytes: String::from("09a024f007c1c1e7996851232fc069916134594eedac467b4668156679287f4ae5e8f369d3150749367bb2d9901f3908351694de3998217d") }
                            },
                            Input {
                                box_id: String::from("6ca2a9d63f2f08663c09d99126ec1be7b65ce2e8f34e283c4d5af78485b47c91"),
                                spending_proof: SpendingProof { proof_bytes: String::from("bb15c60b6d058dfc03cbffac10681f1bb603d79f733d3f1cbe97a9aca7eb059419b472e8d24c979c2df2806bfc9bd7feca8a3158a7841b97") }
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
                                ergo_tree: String::from("100604000400050004000e20002693cd6c3dc7c156240dd1c7370e50c4d1f84a752c2f74d93a20cc22c2899d0e204759889b16a97b0c7ab5ccb30c7fafb7d9e17fd6dc41ab86ae380784abe03e4cd803d601b2a5730000d602e4c6a70407d603b2db6501fe730100ea02d1ededededed93e4c672010407720293e4c67201050ec5720391e4c672010605730293c27201c2a793db63087201db6308a7ed938cb2db6308720373030001730493cbc272037305cd7202"),
                                assets: vec![
                                    Asset {
                                        token_id: String::from("01e6498911823f4d36deaf49a964e883b2c4ae2a4530926f18b9c1411ab2a2c2"),
                                        amount: 1,
                                    }
                                ],
                                creation_height: 599998,
                                additional_registers: serde_json::json!({
                                    "R4": "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
                                    "R5": "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
                                    "R6": "05a4c3edd9998877",
                                }),
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
                                ergo_tree: String::from("1005040004000e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a701730073011001020402d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304"),
                                assets: vec![],
                                creation_height: 599998,
                                additional_registers: serde_json::json!({}),
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
                                ergo_tree: String::from("0008cd03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"),
                                assets: vec![],
                                creation_height: 599998,
                                additional_registers: serde_json::json!({}),
                                transaction_id: String::from(
                                    "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8",
                                ),
                                index: 2,
                            },
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
                                spending_proof: SpendingProof { proof_bytes: String::from("") }
                            }
                        ],
                        data_inputs: vec![],
                        outputs: vec![
                            Output {
                                box_id: String::from(
                                    "98d0271b7a29d62b672d8dd002e38b8cfbfc8e4055a637422b3e9d59cd6ff86d",
                                ),
                                value: 1100000,
                                ergo_tree: String::from("100204a00b08cd029ed28cae37942d25d5cc5f0ade4b1b2e03e18b05c4f3233018bf67c817709f93ea02d192a39a8cc7a70173007301"),
                                assets: vec![],
                                creation_height: 600000,
                                additional_registers: serde_json::json!({}),
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
            extension: Extension {
                header_id: String::from("5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"),
                digest: String::from("778c6259877d559672bcdf195173495bd296ec2f3600aa934b7181df28927e44"),
                // Only some fields
                fields: vec![
                    ExtensionField {
                        key: String::from("0100"),
                        value: String::from("01b0244dfc267baca974a4caee06120321562784303a8a688976ae56170e4d175b"),
                    },
                    ExtensionField {
                        key: String::from("0101"),
                        value: String::from("01557fd0590616b4f6e51eaf54436d61e5585eebfc5a9e860861fc0876064bd3d9"),
                    },
                ]
            },
            ad_proofs: ADProofs {
                header_id: String::from("5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"),
                proof_bytes: String::from("035ce744665e0ead1c6a5b89551420bd604ef83268c8bf826dfc8e7046c24dfe4f03692f140e2e80b969d45d589946043a8c41a3b3d27b1806b39c5c77b39058079b039759043ebacf5b4944fec04e508df040fe7bbfbd83312d2f5ed28a89010d15aa03d43bc000b54c2625c5ef723839989770bced69db09008dcf07388e2a51cf8f70032ed8d07ebd9ce7b000776a162502aa7f6e60ed7b1da5273a110ca424ee41d4180338875e3c328fb7f02b6487edfd1bed1eda894da95168124c1eef7851e7f53b2902029b4bc3967f74ff57a8f6aadbad00a21f988b1a74fb8db0b3871d7e86e10964029c1082731b220b2ac9a6aa819cf13e698bf7a4abb3e9a7100ea12cb4fac20300000050b2fe97c00b0008cd03ff6a2f7c1f7e1395f41eacc733b5060d15fe862eb601b3a937bc69eeca659dafcfdf1f00001ce9cb2aeb260f922571d0824a864f9b1e6fedf1a9952f106ff06b516c1a35accf010001000381ecdbcab5ab4aba3eb5694b35bbb01fb3e2b6ed2593ae5068caca25d415d329ff033fbc2659e611c11fb34e84df172d9f140c1f2a1621fbd2a749f444bf891780cd0003fd2a41f6ae647ae1104a27678a28e8cc95b388cc44828da7bea786a2c91b849d0103969120554b2f8c6e9fe4436877cb99d40d3def17898f82e9ef93183c699e21a9ff03b02e1100da37c93e880107bb32120074c18e883c09404849ed25b2298180b33affff0317812cb30e70e9b55c46dad22ad04b40bc84530d0ee4815e72ace0be1933ac9fff01030a40777056a68a63e1dffa9124ba0382d6f35c6ba46688842e9a930a4402fa09ff039224f3c1117c6b2ca46abbb4ef1df67e0a5631e3526d3296cab40b1cf481f22eff00038a56f2f4f5df4f9cd31e0e3068e91fe4228b90c7e640aa6e23b04d40bf136384ff0383155b134ace02ab25da44bda1f959235d1358eae43f9b03460955cd454e5e60ff037b70543f398d4708f323e0cfe6993eca84ec3feff7ef743ccb4175827f22c9c100035e01b77eb439ca754f99fcc73830b7337cf8c04f55aa6b5f0eb33290d3aa29d1033b7176c241d9212ff574533070d10f6569f75c8500edecf0e104ee226ffb1f7003d32ffda310e770cb908f45bd8d81247d3e4ac65b073c856f56adf8399cea5ab0030845924b46c0330863a66e210f639974eed80d152e7f9cea21b2e3560ef1bb0a03b516917b32ca78b28bd091e5944e33aaadbb5ffb244eda042d1d66a9bed33986037a0c7fb4aaa2f07ec7562ef6cc49339c7dfe65c163d63ecb8739501b2afc2ffc03e4e0681a246656d94532ab078e120cb6f52716a7b383e57ccfa03780766d6852032ff2311f31c0bf88148403e8f00f64583910f26dc291ebf2dfd782cb32d0a261035f940c36bc0faa3afc2b8f23e22185b79b17617774424308686f7e310993adf90222adc1681011b6eb0d41d92ffd5579567b8e830dcd27c1627af844c51247627022adcb39983dfac315ea0fca3bbf56e40f111e201f35a9a95edb64fc7f96e2f300000050a0ae809c81010008cd0343d0731830c77686b86d23a43c30ed79166d4e1d023ec274cf189615fbab6f30ded321000060bf247a44d4e9ebb38ba167687df2774754b4adfcf46b29cbd3b56f491b34760103679379de7710ac16fb71ee757137d83ec2536d63e29ad16c823bd01e0cf6afba0003947429acc426d8fb49164d5582da9313f78fb36b51fb16eecd5127d55a777b4cff00031f544adaf9d143c67bf06d3a5edbf75c01b3fe8706f7483c9ff1da3d7c5afb660000ff01ff03a4db99161486822b284da3642c1de8ee29729bdd544df13aae8b1a061468e493ff03d48e8234cbfd7ffc135298a6ad1cadbadb858476819d1289d05efac3056ad53e0001000001035ecb5c7c29007a93618d499ca62c82c873d624cd2a9d199ea477fe6d23473b0500032de753d6d1804306c062e02f562d4a6815e91f1fb1b7c58b2bc325f21b67c1bbff03a8fdcfab2569209fa038a6479da7865517417fae629bf6aae6695afcc5dde174ff000357d28fa79285e6883a6e7e1beb89d485fa78cdb6bbbcc506e98a3baf447e794303673aff94460d15e4359b2a2ae1ea61558ce8fb9785406e8e5c6fdfd14e3be8c803d2e6302e9d97a04668aa80ebccca1d0042b35455034cbec8efe2bb7550dbb6250330a6fdd0b0d64c45e062259ea2d9e60dc4e370403a5bbe9ef38c609fd47b14a103f171e33e5af30b5c2627d1460430b992b91141572fadd992ce37931bb524099d037da558a4ff3f8aed167c355cd2a8f224573ba911f7b41c07f30aadf78f22c76f026ca287d9cc005e512c5105147e1bdc8149fb4f2c70ca5b8367f51c5d2739643e6ca2a9d63f2f08663c09d99126ec1be7b65ce2e8f34e283c4d5af78485b47c9100000050c1918ac8040008cd02a230c09cf9045621863ab8fd04be84a72714ea4f36a12836e2a9fecd4db2e8d389fc1f000058e04dcba64ec6894fc5592386b0a12245f16b64fbf24040b3d7d2cd237222bf8702026ca2b2cc45c10cabe8c6366f18ff45d6a3c3f98e9a95d40d7f82d78eb6007f7d0000004fcdc58fb00a0008cd03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8bccf240000e4f0d5f789d20ac18f5f1b1874ca20e4ae3d42883770d0426ebeaa81f5635394020003fe21a90d7d4ee616ddd36904911eb004d7cd8a52d8c19b95226ff2d9c55ba63a0000035b77942a455d5b5175700b06396c67073829fdc32ec4e4879bce01c3a5de7dfe0103ef5729f0880288d74abfaea5514e9879eabb727f52b2e17859ce4f462dfe2bf6ff03f8703391f7ca2889d118d57feb486085e7b1a21b53b815da23cd63e78686d11200038de4b6894233c91b85e99ab3ad1e4047bbf75b3b5f4f15df404055bc474a0cca0103a3c5dadf061b4a575fae4592da6d8efe3b20f72fe13ec8f3c084c2eedf72884d031f550e02a39e2f6208a45382c860c68729fcd3800c043988289f57d9b453151603c7d207d8171cb767b02a0846bea932226812890f200f3bf4db405fda4c9bfc39026cb8fc34b33c846ff54c2cc546f306a05c93e9e268683cdf40594fba2e7da8f46cb916866bad5cf009386094da00a365e98b103be27043e08a46fe0db02d652b00000050aa9ddbfeaf010008cd022d591eb6824fb183889ea51f12db99980b27ac388607fdff5ef4634879e8613bb0c21e0000ae22a30152d39f8bb02d8ef518ee82f5694cbffd5dff1fbc64f5b0dec9f3d88e02ff000309bd6f4cd038c84031b3d24c85c9a060d93395b04252152d3df6c87bc689b51cff03abb1d5a3a83486881a3b1c36f940d853a4855b97385caf22adff261aa86bd68e00031fa2e64adb871bea976506fe5c83498213bae284076f098b0aeafb13991caf58ff039577f41ae7ef43f67385ae188aac65d4831098504cb55a79daf403524858605601010003e828e89506359a72c42ce0bbae47b8d197c6347a6099bb9f33d4aa727618bee0ff0103e8384622fde648558b6818ef53149119066d3b4cc451832dd2618dd10be54b3bff03f98d85aece03015e339a07d982b23fcb800334d42ef7d0ac95ebe0e948910cd5000103aa2ba297cd5dc401954cbb214aa21f03f83f39b03a0e492e30d89a4988e60b8e0000ff030c5ee79b29d4fdc264d3ce444955bcc3d988de9cb353b7ee9e613e49efb5a59bff000103659fd79b109df57baeb769bbcf436c5d3ee2d8cfa07327b69c8ea79ac994695a03755ec9d7199e3446c9eac57528c5b4fd0299961ad434918173ba6cd2aabedd3703710d1eba692ba1e684d7f63cfb832679b701525814af4bf5fbd7415c42d8ae4303b0b5b1ff4bbaa92c9689774f58a41512afceb8a95eb12ee2b36136615ba35e7e0301841efc8831c6377d95e08d0d3cdc67e13c070fa48162f15eccf31aa8fddfcc03c6e386080315987b9bca12344439f0f944cec8fa84d768ad59eddfa33e580c9503aadd003e93735efddb3c39e4fe3915cdd4b18a63af73324d995cd2bac0a9cbf203c28d44c5230b42d9913d11f31c1870bef16a9ad70d33830cbdbd8085f447414a03abb6db5c968afc37eb7d1e91b84a1199fadd35c6eb4ef9d01458454b5c8428130298479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb89847aa14dc51e2e95cec7215512b081b5a1cdcfcbf57c7785fac5021c57bbee300000219e0b6f9c7061014040004000e2001e6498911823f4d36deaf49a964e883b2c4ae2a4530926f18b9c1411ab2a2c204000400040204020101040205c096b102040204040500040c040204c096b1020402050a05c8010402d806d601b2a5730000d602b5db6501fed9010263ed93e4c67202050ec5a7938cb2db63087202730100017302d603b17202d604e4c6b272027303000605d605d90105049590720573047204e4c6b272029972057305000605d606b07202860273067307d901063c400163d803d6088c720601d6098c720801d60a8c72060286029a72097308ededed8c72080293c2b2a5720900d0cde4c6720a040792c1b2a5720900730992da720501997209730ae4c6720a0605ea02d1ededededededed93cbc27201e4c6a7060e927203730b93db63087201db6308a793e4c6720104059db07202730cd9010741639a8c720701e4c68c72070206057e72030593e4c6720105049ae4c6a70504730d92c1720199c1a77e9c9a7203730e730f058c72060292da720501998c72060173109972049d9c720473117312b2ad7202d9010763cde4c672070407e4c6b2a5731300040400bccf2401002693cd6c3dc7c156240dd1c7370e50c4d1f84a752c2f74d93a20cc22c2899d010305c0e4b4b089977704fe9e490e209ba1e096be239743865fbf586fc27a94a05d985129d9693076936d369aa5d644e4f0d5f789d20ac18f5f1b1874ca20e4ae3d42883770d0426ebeaa81f563539400000308ba8733d56beed8d9a125e62d6e409223c7b21a92bf6311d7bfa82dba7ad73b00000313268f72a2e012bc778ab7df0c9951f32afcda486748485d9da5d79f50b3b7960003a5f6c7a310e2aaf9eff09de29f3df60c26f41f6c6a0c9c10d3edd5f1bacd04880000000386c05c8740e05a4c9b1c1177da22e2d0c73c36eaec06c08544ca09c47b3c480500ff038f214d97a0274feea36a7f758a8b9b12d431339146c15349b6e3f8595554d5a60003dffc8f75d7cb1abc3d7698e395f5675c4b76c1d5a7f44fa22f2d3e03791fefef0103bf26283d7d1b7e86b6e11551ca59b36405d36ec3f8549384e3dab9439c2311b603c8fef1cb9428731780e2a2fe0a9a4a9d8822033e62e843d8b77e12d711f4c2500298d01c9988907684d2edb7b372ce8901ac4dd2689473461b146b7916df33aa2b98d029cb12d3a7e72206072a0f1ad48cb44baceed29bc790259d6df1794c6a9f0000004f9ac3f9a4020008cd034a53f17d249721c647c13477bb16982c8b2b16daa923d2a49dee8a88593c8356fd84240000313218da5aa65743df2b241f77af32e5104b9489504683cd44cad2187fdfdd540d0369647970739bcef90309447ee86fc86d59eff7ffde20bab7e4ee2551918ca466010383e54971274b49300a4919f7b2a7689b670739023e2480a9f1d6532fdc56a105ff038c9f24afc7756379e4eb6757fa4e49713cd298cfb2bd34ac23d9abccbb57bd3500ff03cd7c5243011677dd8f476a3094331f33505ca1b5613941b63fda0ffd986af92e0003f73d84d3af27d1a6aa1eec08eefc7cb8ac9eea28f34002d68140a2c03e80ecfbff03af7b1628a7c817d0c1c3c685bb7b45d01ab4d70e3607e8119c5935d6b3b0865aff0103efa3d020d2df2ae1428879eafd29e62c740f391c80b1b30f32aedfc89ebb54bc01030b95720f31437ec5eee528ad465b57883e77da1c8e66c8a8e921dc2616de8c57000000ff01000385530f8cf5f7ae06afee27ecbea5bda9a362057939172cc19e9713d9fb8b5b33ff0393104ed20a0c3d3a4f80bd83792ec9e3755abeb1df84fb8d17782e29e26405f1039d5ad091b8d208ef742ec56764a47afa0af83bc3b89aa048a1e4c6121a9ebe55033264f36edfca9d341f4e7cc5d9b6453fdb124253de6ca53d2cea31cdb6d1279a03799778021940edf6e9ca7df7a94bdf0455cb252ee7794b325b4de00dd0c60b1103fe027898d8d63183a2c563954b112494a3a62612e69b0dd459601f41b24492bc032bc574eca03d44cd3c4040956825a9c7e37a2ba86c2eeee35fb537651a4f41e202aa94156da4d0486432a9dbc8e383af622ffcdfa9d073a087cdc0dca593f7ecd6aa942399f9310690c035ca8527967281f4581d99cace9bc1620358ba8bed49c10000004ed7fafe3a0008cd0268d78c48a2f1b8b768aa4dc394e769fe0b1cbd3b6bb3cbeb17db15a060f90455e89e2400004bde949bae687974cf0016a2c8f2f16b8a97fd201bb864c204b70da0a50c7d380b0003626d453794077b56bbb3f1949c4e4ba7d0522cfe0e686fa5a52de5f54f4d054b00ff036eb1cbe7780090e3c863ac633c8e8c625f2f002bbd99ba29b87c66b1cdc7780c000388f0eb9a7ffbcf013a29d54bff1e23398b10a192c3f0c6d7ea1382c42e41e4ca0100ff030e99337f949188edef66b134efad3853d8e2424b393bfe0df768823ccacc0cf3000371d4dc4407bd497bc66c292eb8bff39bf14a0cec504374ac30f6a8ee140dce5f000000036de6093a047e1434e67f5bdcd0f3ae92faec84e004308d75d187c78ea223fb0600038d1c5466086d3c5489ee691d776b2130532f7e1bd42e1445b3252c9209b7f5c600037f51ffe7b2019d48dd228c416f411787274ab5bb1bc0032de6b178f9225cdc6b0003533fc57bb646be1acdecb77f32837a52c52d72e85c382229649e4a05e1f796010003959e67bcb58ee6d7d7c4d1d77377e96c86a5f9a77b7dce8285e92f7371ce06090381ad1eaafceb978f516d45b29859da8b0018d7a9b4a89b6a36d773080a4c33ab03894667a761f6cf4a40f936695151c0f972b259689bf7a02bab6531b519b4c9cd03f2ebd1cff0504013a72878b0ad758ffc24449638e29bee8ba573878bac5a8e3e03f81cb0b414ba28cee9508875d3a1471f7498bca8347a8eed3b5451e96e0c7ae0032359de2124898a66adcbb5ba224f545f413d386244822785f93c990fe41f7813031092eb7d9f42e50374c48144a6f3f938b1fb4dd5f57a6f737b0e8a4120222522034220bc63726a91b6f8d5e2441f391637786ebee63555a99b2f9efddd22427e530370a06069fbe73830ef8ae46a8d7a89df7c65459a1a542e0b3d1ce4196e51485402c7397a34ef31d835ce30afa4f9dcb70fbf3388478a826eb5e81d75643dce87f8c739a3294d592377a131840d491bd2b66c27f51ae2c62c66be7bb41b248f321e0000004faafbdde1030008cd03c7c1250accfa516a5754b156aa2feaff76d94f0e9e49c2cbb0a58245dd11700ce5af1f0000d59ce0f38838b1c2e5a99efea7f70086c60ecc5c8ea3667901c31fad18e3f6731502c739bb8e9e6e4b22de472086e14a822f509c8b25af46eee22d4a818bfe07795f0000014bc0843d100604000400050004000e20002693cd6c3dc7c156240dd1c7370e50c4d1f84a752c2f74d93a20cc22c2899d0e204759889b16a97b0c7ab5ccb30c7fafb7d9e17fd6dc41ab86ae380784abe03e4cd803d601b2a5730000d602e4c6a70407d603b2db6501fe730100ea02d1ededededed93e4c672010407720293e4c67201050ec5720391e4c672010605730293c27201c2a793db63087201db6308a7ed938cb2db6308720373030001730493cbc272037305cd7202b4cf240101e6498911823f4d36deaf49a964e883b2c4ae2a4530926f18b9c1411ab2a2c201030703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae80e2085f8cc493af00c89d1370da75665728ef01dc14960df8e6ee1622e01c1f08ad505bce6adafd999776404e9363497173bf7aa5087f04cf0e9aeb997ff8af74ac02fecbc174cf9ffbc0000038d8d93ad9c82b0d8625d46834f8040d1f877d7752367852dce6cb325c23b738dff0322a82959869c0f0a2495f94761a8c993508ecdec91351d3e92d31b8907a83d98ff03ec2e2b404219cde8e793a90047f8539de95ddf6e9265e8fabe915f5d85f46deeff01030867bbb3d5ac2ce11b6f0aaa5a9be63a445f71af4ef7fbe324da84c39b4d5e1dff03ec2aae005660da040cf5ec61f9ca87c43dc87eeb65811e0ec42ed48c4080f225ff010003f265cf03f5dea403a988a65b006b9761df2d300deb950cd91ab6e36209afcc12ff000103b22e89d9459f8f268afb3d99b4a0fc84ebbe4e3246d204aaccc3d92c9e667ca8ff000000ff00ff033455a1ff32f2552fce366fa427031e04af3113a99b99062e5ac1401aee4885ea03ba31f34501c5c612019c7c50925266eefbe1c609f8deb5b69588eb047078e202030350f315cea3942424b2d9e97db429d07115b16b37b96053c2a378ade3298ec103844c51cc8a459cd2c8b94c37334369a166bfb497682bfa6cbd68577a65320b80036c44e655c572acd6bf3636d1d4ab4534ccf66416b19e41ebd623562d4e6af1a303efab55421746d0d5d318537508c22c04f8cbd52862a9a84386a44ece30b2bd9002eb1c2fb378e1f96b7ee10eb25b1398cffbd231f4f83c5ac777aa76292a0a0478eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f0000005080e8eda1ba010008cd020de0bf30865182e342c0d7f3bceed0e5ba97bbe7fb62661c4f8584f4f250ec89eb81170000277de14928e52b6da17a5880f919e75d266f4ddb39987a9b95e4e0301fd9fdce0002eb1c55f1ca9363807c55cfc6967c9baa58160918428ebc9ee337745e09600a850000011280c0e6bab194fe5d101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730fbfcf240000a00ebfb89b3416403a3d2c6bd749c4181b873ca19bda61350346833da87707d600000312f0f8a70b735fa84d7e27b18e224818b3addab3bb13908540a3e927edc37f8fff031dda2454add94a8c891cde06c04c981f1b804b0576546a0188617b26a673bcca0003b10456199abc1270e28ec0c2338e001667f7c6f7874f17c91f58dbd86a64d98a00ff0344911c581911ab9d04ffb420897a32e0c20027dec071897267cd1fba8e663e27ff0103222f7f629ad7188ae0396b9ec7b78b36d408740c22fedb8400bcc06499d99c69ff00038d0f5117af893420dc7e830a12f4e27d221fd4caf457d7b83d96b276b019117bff031572a472ef79966db56d8333e9b5814a93f87c71074c6fa5ba3f38bac7f475bfff03851c140d1f075faadbb875fa82439c6fa10b4b6bb197842de0fd79077b0830b800037f9828376b9319a3624a9ff693fbc65a230b18ba2b1903338a5ac448aff9e5c300032ea5ffdb1b2b6b47135d9b33dec12cd97af1d43899f272d612866eb6b88459cb010001036449aa1b96eefedf84205d0e7cf76fa01991de91df7440c0ea495b488cb481000000ff00040e9795b45f02c90ef9ea7eeb6387a19396c7c1eefacc02"),
                digest: String::from("f391048a64b318f8a0e0b9380e94c546c0eaddabb6a10dd0daa48a04801b1a26"),
                size: 7112
              },
            size: 8486,
        }
    }

    
    pub fn genesis_boxes() -> Vec<Output> {
        vec![
            Output {
                box_id: String::from(
                    "b69575e11c5c43400bfead5976ee0d6245a1168396b2e2a4f384691f275d501c",
                ),
                value: 93409132500000000,
                ergo_tree: String::from("101004020e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a7017300730110010204020404040004c0fd4f05808c82f5f6030580b8c9e5ae040580f882ad16040204c0944004c0f407040004000580f882ad16d19683030191a38cc7a7019683020193c2b2a57300007473017302830108cdeeac93a38cc7b2a573030001978302019683040193b1a5730493c2a7c2b2a573050093958fa3730673079973089c73097e9a730a9d99a3730b730c0599c1a7c1b2a5730d00938cc7b2a5730e0001a390c1a7730f"),
                assets: vec![],
                creation_height: 0,
                additional_registers: serde_json::json!({}),
                transaction_id: String::from(
                    "0000000000000000000000000000000000000000000000000000000000000000",
                ),
                index: 0,
            },
            Output {
                box_id: String::from(
                    "b8ce8cfe331e5eadfb0783bdc375c94413433f65e1e45857d71550d42e4d83bd",
                ),
                value: 1000000000,
                ergo_tree: String::from("10010100d17300"),
                assets: vec![],
                creation_height: 0,
                additional_registers: serde_json::json!({
                    "R4": "0e4030303030303030303030303030303030303031346332653265376533336435316165376536366636636362363934326333343337313237623336633333373437",
                    "R5": "0e42307864303761393732393334363864393133326335613261646162326535326132333030396536373938363038653437623064323632336337653365393233343633",
                    "R6": "0e464272657869743a20626f746820546f727920736964657320706c617920646f776e207269736b206f66206e6f2d6465616c20616674657220627573696e65737320616c61726d",
                    "R7": "0e54e8bfb0e8af84efbc9ae5b9b3e8a1a1e38081e68c81e7bbade38081e58c85e5aeb9e28094e28094e696b0e697b6e4bba3e5ba94e5afb9e585a8e79083e58c96e68c91e68898e79a84e4b8ade59bbde4b98be98193",
                    "R8": "0e45d094d0b8d0b2d0b8d0b4d0b5d0bdd0b4d18b20d0a7d0a2d09fd09720d0b2d18bd180d0b0d181d182d183d18220d0bdd0b02033332520d0bdd0b020d0b0d0bad186d0b8d18e",
                }),
                transaction_id: String::from(
                    "0000000000000000000000000000000000000000000000000000000000000000",
                ),
                index: 0,
            },
            Output {
                box_id: String::from(
                    "5527430474b673e4aafb08e0079c639de23e6a17e87edd00f78662b43c88aeda",
                ),
                value: 4330791500000000,
                ergo_tree: String::from("100e040004c094400580809cde91e7b0010580acc7f03704be944004808948058080c7b7e4992c0580b4c4c32104fe884804c0fd4f0580bcc1960b04befd4f05000400ea03d192c1b2a5730000958fa373019a73029c73037e997304a305958fa373059a73069c73077e997308a305958fa373099c730a7e99730ba305730cd193c2a7c2b2a5730d00d5040800"),
                assets: vec![],
                creation_height: 0,
                additional_registers: serde_json::json!({
                    "R4": "0e6f98040483030808cd039bb5fe52359a64c99a60fd944fc5e388cbdc4d37ff091cc841c3ee79060b864708cd031fb52cf6e805f80d97cde289f4f757d49accf0c83fb864b27d2cf982c37f9a8b08cd0352ac2a471339b0d23b3d2c5ce0db0e81c969f77891b9edf0bda7fd39a78184e7",
                }),
                transaction_id: String::from(
                    "0000000000000000000000000000000000000000000000000000000000000000",
                ),
                index: 0,
            },
        ]
    }
}

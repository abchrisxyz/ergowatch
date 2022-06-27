mod ergo_tree;
mod register;
mod votes;

use ergotree_ir::chain::ergo_box::ErgoBox;
use ergotree_ir::chain::ergo_box::NonMandatoryRegisters;
use ergotree_ir::chain::ergo_box::NonMandatoryRegisterId;
use ergotree_ir::mir::constant::Constant;
use ergotree_ir::base16_str::Base16Str;
use ergotree_ir::serialization::SigmaSerializable;
use log::debug;
use crate::node;


/// A preprocessed version of block data provided by the node.
///
/// Used as input to every processing unit.
///
/// - Decodes address bytes to strings.
/// - Casts unsigned ints to signed (for postgres compatibility)
#[derive(Debug)]
pub struct BlockData<'a> {
    pub height: i32,
    pub header_id: &'a str,
    pub parent_header_id: &'a str,
    pub timestamp: i64,
    pub votes: [i8;3],
    pub transactions: Vec<Transaction<'a>>,
}

impl<'a> BlockData<'a> {
    pub fn new(block: &'a node::models::Block) -> Self {
        BlockData {
            height: block.header.height as i32,
            header_id: &block.header.id,
            parent_header_id: &block.header.parent_id,
            timestamp: block.header.timestamp as i64,
            votes: votes::from_str(&block.header.votes),
            transactions: block
                .block_transactions
                .transactions
                .iter()
                .enumerate()
                .map(|(i, tx)| Transaction::from_node_transaction(tx, i))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    pub id: &'a str,
    pub index: i32,
    pub outputs: Vec<Output>,
    pub input_box_ids: Vec<&'a str>,
    pub data_input_box_ids: Vec<&'a str>,
}

impl<'a> Transaction<'a> {
    fn from_node_transaction(tx: &'a node::models::Transaction, index: usize) -> Self {
        debug!("Processing transaction {}", &tx.id);
        Transaction {
            id: &tx.id,
            index: index as i32,
            outputs: tx
                .outputs
                .iter()
                .map(|op| Output::from_ergo_box(&op))
                .collect(),
            input_box_ids: tx.inputs.iter().map(|i| &*i.box_id).collect(),
            data_input_box_ids: tx.data_inputs.iter().map(|d| &*d.box_id).collect(),
        }
    }
}

#[derive(Debug)]
pub struct Output {
    pub box_id: String,
    pub creation_height: i32,
    pub address: String,
    pub index: i32,
    pub value: i64,
    pub additional_registers: [Option<Register>; 6],
    pub assets: Vec<Asset>,
    pub size: i32,
}

impl Output {
    pub fn from_ergo_box(eb: &ErgoBox) -> Self {
        debug!("Processing output {:?}", eb.box_id());
        Output {
            box_id: String::from(eb.box_id()),
            creation_height: eb.creation_height as i32,
            address: ergo_tree::address_from_ergo_tree(&eb.ergo_tree),
            index: eb.index as i32,
            value: eb.value.as_i64(),
            additional_registers: parse_additional_registers(&eb.additional_registers),
            assets: match &eb.tokens {
                Some(ts) => {
                    ts.iter().map(|t| Asset {
                        token_id: String::from(t.token_id.clone()),
                        amount: i64::from(t.amount),
                    })
                    .collect()
                },
                None => vec![]
            },
            size: eb.sigma_serialize_bytes().unwrap().len() as i32,
        }
    }
}

impl Output {
    pub fn r4(&self) -> &Option<Register> {
        &self.additional_registers[0]
    }
    pub fn r5(&self) -> &Option<Register> {
        &self.additional_registers[1]
    }
    pub fn r6(&self) -> &Option<Register> {
        &self.additional_registers[2]
    }
    // Enable when needed
    // pub fn r7(&self) -> &Option<Register> {
    //     &self.additional_registers[3]
    // }
    // pub fn r8(&self) -> &Option<Register> {
    //     &self.additional_registers[4]
    // }
    // pub fn r9(&self) -> &Option<Register> {
    //     &self.additional_registers[5]
    // }
}

#[derive(Debug)]
pub struct Register {
    pub id: i16,
    pub stype: String,
    pub serialized_value: String,
    pub rendered_value: String,
}

fn parse_additional_registers(regs: &NonMandatoryRegisters) -> [Option<Register>; 6] {
    if regs.is_empty() {
        return [None, None, None, None, None, None];
    }
    [
        match regs.get(NonMandatoryRegisterId::R4) {
            Some(cst) => parse_register(cst, 4),
            None => None,
        },
        match regs.get(NonMandatoryRegisterId::R5) {
            Some(cst) => parse_register(cst, 5),
            None => None,
        },
        match regs.get(NonMandatoryRegisterId::R6) {
            Some(cst) => parse_register(cst, 6),
            None => None,
        },
        match regs.get(NonMandatoryRegisterId::R7) {
            Some(cst) => parse_register(cst, 7),
            None => None,
        },
        match regs.get(NonMandatoryRegisterId::R8) {
            Some(cst) => parse_register(cst, 8),
            None => None,
        },
        match regs.get(NonMandatoryRegisterId::R9) {
            Some(cst) => parse_register(cst, 9),
            None => None,
        },
    ]
}

fn parse_register(cst: &Constant, id: i16) -> Option<Register> {
    let rendered_register = register::render_register_value(&cst);
    Some(Register {
        id: id,
        stype: rendered_register.value_type,
        // The const was deserialized from json, so safe to serialize again.
        serialized_value: cst.base16_str().unwrap(),
        rendered_value: rendered_register.value,
    })
}

#[derive(Debug)]
pub struct Asset {
    pub token_id: String,
    pub amount: i64,
}

impl std::ops::Add for Asset {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        assert_eq!(self.token_id, other.token_id);
        Self {
            token_id: self.token_id,
            amount: self.amount + other.amount,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::Asset;
    use super::BlockData;
    use super::Output;
    use super::Transaction;
    use crate::node::models::testing::block_600k;
    use pretty_assertions::assert_eq;

    #[test]
    fn header_info() -> () {
        let node_block = block_600k();
        let block = BlockData::new(&node_block);
        assert_eq!(block.height, node_block.header.height as i32);
        assert_eq!(block.header_id, node_block.header.id);
        assert_eq!(block.parent_header_id, node_block.header.parent_id);
        assert_eq!(block.timestamp, node_block.header.timestamp as i64);
        assert_eq!(
            block.transactions.len(),
            node_block.block_transactions.transactions.len()
        );
    }

    #[test]
    fn transaction_from_node_tx() {
        let index = 1usize;
        let node_tx = &block_600k().block_transactions.transactions[index];
        let tx = Transaction::from_node_transaction(&node_tx, index);
        assert_eq!(tx.id, node_tx.id);
        assert_eq!(tx.outputs.len(), node_tx.outputs.len());
        assert_eq!(tx.input_box_ids.len(), node_tx.inputs.len());
        assert_eq!(tx.data_input_box_ids.len(), node_tx.data_inputs.len());
    }

    #[test]
    fn output_from_node_output() {
        let ergo_box = &block_600k().block_transactions.transactions[1].outputs[0];
        let output = Output::from_ergo_box(&ergo_box);
        assert_eq!(output.box_id, String::from(ergo_box.box_id()));
        assert_eq!(output.creation_height, ergo_box.creation_height as i32);
        assert_eq!(output.index, ergo_box.index as i32);
        assert_eq!(output.value, ergo_box.value.as_i64());
        assert_eq!(output.address, "jL2aaqw6XU61SZznvcri5VZnx1Gn8hfZWK87JH6PM7o1YMDMZfpH1uoGJSd3gDQabX6AmCZKLyMSBqSoUAo8X7E5oNRV9JgCdLBFjV6i1BEjZLwgGo3RUr4p8zchqrJ1FeGPLf2DidW6F41aeM1zCM64ZjfBqcy8d6fgEnAn53W28GEDQi5W1XCWRjFvgTFuDdAzd6Yj65KGJhdvMSgffP7pELpCtqK5Z4dX9SQKtt8Y4RMBaeEKtKB1pEx1n");
    }

    #[test]
    fn output_registers() {
        let ergo_box = &block_600k().block_transactions.transactions[1].outputs[0];
        let output = Output::from_ergo_box(&ergo_box);
        
        let r4 = &output.r4().as_ref().unwrap();
        assert_eq!(r4.id, 4);
        assert_eq!(r4.stype, "SGroupElement");
        assert_eq!(r4.serialized_value, "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8");
        assert_eq!(r4.rendered_value, "03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8");
        
        let r5 = &output.r5().as_ref().unwrap();
        assert_eq!(r5.id, 5);
        assert_eq!(r5.stype, "Coll[SByte]");
        assert_eq!(r5.serialized_value, "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8");
        assert_eq!(r5.rendered_value, "98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8");
        
        let r6 = &output.r6().as_ref().unwrap();
        assert_eq!(r6.id, 6);
        assert_eq!(r6.stype, "SLong");
        assert_eq!(r6.serialized_value, "05a4c3edd9998877");
        assert_eq!(r6.rendered_value, "261824656027858");
    }

    #[test]
    fn output_assets() {
        let ergo_box = &block_600k().block_transactions.transactions[1].outputs[0];
        let output = Output::from_ergo_box(&ergo_box);
        assert_eq!(output.assets.len(), 1usize);
    }

    #[test]
    fn test_assets_with_same_token_id_can_be_added() {
        let bag_a = Asset {
            token_id: String::from("token_id"),
            amount: 1000,
        };
        let bag_b = Asset {
            token_id: String::from("token_id"),
            amount: 2000,
        };
        let total = bag_a + bag_b;
        assert_eq!(total.token_id, "token_id");
        assert_eq!(total.amount, 3000);
    }

    #[test]
    #[should_panic]
    #[allow(unused)]
    fn test_adding_assets_with_different_token_id_panics() {
        let bag_a = Asset {
            token_id: String::from("token_id"),
            amount: 1000,
        };
        let bag_b = Asset {
            token_id: String::from("other_token_id"),
            amount: 2000,
        };
        bag_a + bag_b;
    }
}

#[cfg(test)]
pub mod testing {
    use super::Asset;
    use super::BlockData;
    use super::Output;
    use super::Register;
    use super::Transaction;

    /// Returns a BlockData with contents of block 600k (dummy box sizes)
    pub fn block_600k<'a>() -> BlockData<'a> {
        let tx_1 = Transaction {
            id: "4ac89169a2f83adb895b3d76735dbcfc63ad7940bddc2492d9ee4201299bf927",
            index: 0,
            outputs: vec![Output {
                box_id: String::from("029bc1cb151aaef51c3678d2c74f3e82c9f4d197dd37e7a4eb73612f9da4f1f6"),
                creation_height: 600000,
                address: String::from("2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU"),
                index: 0,
                value: 52909132500000000,
                additional_registers: [None, None, None, None, None, None],
                assets: vec![],
                size: 110,
            }, Output {
                box_id: String::from("6cb8ffe391838b627cb893c9b2027aa2a03f3a20455dd11e5ac903c7e4179ace"),
                creation_height: 600000,
                address: String::from("88dhgzEuTXaRvR2VKsnXYTGUPh3A9VK8ojeRcpHihcrBu23dnwbB12BbVcJuTcdGfRuSzA8bW25Az6n9"),
                index: 1,
                value: 67500000000,
                additional_registers: [None, None, None, None, None, None],
                assets: vec![],
                size: 120,
            }],
            input_box_ids: vec!["eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f"],
            data_input_box_ids: vec![],
        };
        let tx_2 = Transaction {
            id: "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8",
            index: 1,
            outputs: vec![
                Output {
                    box_id: String::from("aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d"),
                    creation_height: 599998,
                    address: String::from("jL2aaqw6XU61SZznvcri5VZnx1Gn8hfZWK87JH6PM7o1YMDMZfpH1uoGJSd3gDQabX6AmCZKLyMSBqSoUAo8X7E5oNRV9JgCdLBFjV6i1BEjZLwgGo3RUr4p8zchqrJ1FeGPLf2DidW6F41aeM1zCM64ZjfBqcy8d6fgEnAn53W28GEDQi5W1XCWRjFvgTFuDdAzd6Yj65KGJhdvMSgffP7pELpCtqK5Z4dX9SQKtt8Y4RMBaeEKtKB1pEx1n"),
                    index: 0,
                    value: 1000000,
                    additional_registers: [
                        Some(Register {
                            id: 4,
                            stype: String::from("SGroupElement"),
                            serialized_value: String::from("0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"),
                            rendered_value: String::from("03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"),
                        }),    
                        Some(Register {
                            id: 5,
                            stype: String::from("Coll[SByte]"),
                            serialized_value: String::from("0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"),
                            rendered_value: String::from("98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"),
                        }),
                        Some(Register {
                            id: 6,
                            stype: String::from("SLong"),
                            serialized_value: String::from("05a4c3edd9998877"),
                            rendered_value: String::from("261824656027858"),
                        }),
                        None, None, None],
                    assets: vec![
                        Asset {
                            token_id: String::from("01e6498911823f4d36deaf49a964e883b2c4ae2a4530926f18b9c1411ab2a2c2"),
                            amount: 1,
                        }
                    ],
                    size: 210,
                },
                Output {
                    box_id: String::from("5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4"),
                    creation_height: 599998,
                    address: String::from("2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"),
                    index: 1,
                    value: 1100000,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![],
                    size: 220,
                },
                Output {
                    box_id: String::from("22adc6d1fd18e81da0ab9fa47bc389c5948780c98906c0ea3d812eba4ef17a33"),
                    creation_height: 599998,
                    address: String::from("9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b"),
                    index: 2,
                    value: 2784172525,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![],
                    size: 230,
                }
            ],
            input_box_ids: vec![
                "c739a3294d592377a131840d491bd2b66c27f51ae2c62c66be7bb41b248f321e",
                "6ca2a9d63f2f08663c09d99126ec1be7b65ce2e8f34e283c4d5af78485b47c91",
            ],
            data_input_box_ids: vec!["98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"],
        };
        let tx_3 = Transaction {
            id: "db3d79ab228b1b93bcb8cd742bacb0a4b49ad5fe67cc11b495482b8c541d3ae2",
            index: 2,
            outputs: vec![Output {
                box_id: String::from("98d0271b7a29d62b672d8dd002e38b8cfbfc8e4055a637422b3e9d59cd6ff86d"),
                creation_height: 600000,
                address: String::from("2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"),
                index: 0,
                value: 1100000,
                additional_registers: [None, None, None, None, None, None],
                assets: vec![],
                size: 310,
            }],
            input_box_ids: vec!["5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4"],
            data_input_box_ids: vec![],
        };
        BlockData {
            height: 600000,
            header_id: "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4",
            parent_header_id: "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1",
            timestamp: 1634511451404,
            votes: [0, 0, 0],
            transactions: vec![tx_1, tx_2, tx_3],
        }
    }

    /// Returns a BlockData with token minting transactions.
    /// Fictive block based off block 600k
    pub fn block_minting_tokens<'a>() -> BlockData<'a> {
        // minting 5000 best tokens
        let tx_1_minting_eip4_tokens = Transaction {
            id: "075574e63e1f18d3f66398e256f581ef6d22a704801f25665a772d0d0b7914e2",
            index: 0,
            outputs: vec![
                Output{
                    box_id: String::from("5410f440002d0f350781463633ff6be869c54149cebeaeb935eb2968918e846b"),
                    creation_height: 114626,
                    address: String::from("9ggm43XYvHgqp2DfAuqdPoFJ9UgG33Y3fDrk9ydkH9h9k15eGwK"),
                    index: 0,
                    value: 100000,
                    additional_registers: [
                        Some(Register {
                            id: 4,
                            stype: String::from("Coll[SByte]"),
                            serialized_value: String::from("0e0462657374"),
                            rendered_value: String::from("62657374"),
                        }),
                        Some(Register {
                            id: 5,
                            stype: String::from("Coll[SByte]"),
                            serialized_value: String::from("0e057465737420"),
                            rendered_value: String::from("7465737420"),
                        }),
                        Some(Register {
                            id: 6,
                            stype: String::from("Coll[SByte]"),
                            serialized_value: String::from("0e0131"),
                            rendered_value: String::from("31"),
                        }),
                        None, None, None],
                    assets: vec![
                        Asset {
                            token_id: String::from("34d14f73cc1d5342fb06bc1185bd1335e8119c90b1795117e2874ca6ca8dd2c5"),
                            amount: 5000,
                        }
                    ],
                    size: 110,
                }, Output {
                    box_id: String::from("bbb7d9e0333007ff5005771dccfe11c309a98df99c0cf10e17c60e64cb7ccc5b"),
                    creation_height: 114626,
                    address: String::from("2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"),
                    index: 1,
                    value: 1000000,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![],
                    size: 120,
                }, Output {
                    box_id: String::from("b5d971fa03de96b5bfbdff9dba76c519ed0f1f8196a01c139c6be74a9c47040a"),
                    creation_height: 114626,
                    address: String::from("9ggm43XYvHgqp2DfAuqdPoFJ9UgG33Y3fDrk9ydkH9h9k15eGwK"),
                    index: 2,
                    value: 31134600,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![],
                    size: 130,
            }],
            input_box_ids: vec![
                "34d14f73cc1d5342fb06bc1185bd1335e8119c90b1795117e2874ca6ca8dd2c5",
                "7acc484210f3459217ba3d5549aef99b1a6fd7fec54943e8e3a909784f36ee1f"
            ],
            data_input_box_ids: vec![],
        };
        // minting 1000 non-eip4 compliant tokens (decimals provided as int)
        let tx_2_minting_non_compliant_eip4_token = Transaction {
            id: "91f3e120b62a6848521d3eff1601d251682df8181a95052050c23610ce6b18ee",
            index: 1,
            outputs: vec![
                Output {
                    box_id: String::from("48461e901b2a518d66b8d147a5282119cfc5b065a3ebba6a56b354686686a48c"),
                    creation_height: 106481,
                    address: String::from("9fjo2FEBvkpJkq7TB5eaqcT3zUcokDRSL4JaGpEonLr9cS1JZZ2"),
                    index: 0,
                    value: 50760,
                    additional_registers: [
                        Some(Register {
                            id: 4,
                            stype: String::from("Coll[SByte]"),
                            serialized_value: String::from("0e06617070545354"),
                            rendered_value: String::from("617070545354"),
                        }),
                        Some(Register {
                            id: 5,
                            stype: String::from("Coll[SByte]"),
                            serialized_value: String::from("0e0a5465737420746f6b656e"),
                            rendered_value: String::from("5465737420746f6b656e"),
                        }),
                        Some(Register {
                            id: 6,
                            stype: String::from("SInt"),
                            serialized_value: String::from("0410"),
                            rendered_value: String::from("8"),
                        }),
                        None, None, None],
                    assets: vec![
                        Asset {
                            token_id: String::from("3c65b325ebf58f4907d6c085d216e176d105a5093540704baf1f7a2a42ad60f8"),
                            amount: 1000,
                        }
                    ],
                    size: 210,
                },
                Output {
                    box_id: String::from("51c38dad38332ca22508f7614568f31b62fb5ccd09b5287734f2152ef8c04360"),
                    creation_height: 106481,
                    address: String::from("2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"),
                    index: 1,
                    value: 1000000,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![],
                    size: 220,
                },
                Output {
                    box_id: String::from("f6fa1d664ca8153f4b696453ef1e7b18c75de67cce1237312d1ce39349cc7160"),
                    creation_height: 599998,
                    address: String::from("9fjo2FEBvkpJkq7TB5eaqcT3zUcokDRSL4JaGpEonLr9cS1JZZ2"),
                    index: 2,
                    value: 1998949240,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![],
                    size: 230,
                }
            ],
            input_box_ids: vec!["3c65b325ebf58f4907d6c085d216e176d105a5093540704baf1f7a2a42ad60f8"],
            data_input_box_ids: vec![],
        };
        BlockData {
            height: 600000,
            header_id: "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4",
            parent_header_id: "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1",
            timestamp: 1634511451404,
            votes: [0, 0, 0],
            transactions: vec![
                tx_1_minting_eip4_tokens,
                tx_2_minting_non_compliant_eip4_token,
            ],
        }
    }

    /// Based on tx 0f778e5c5c2ecb8b26d2b7f739456e8942f3785ea1228ecfc0edda62e86b9177
    /// Contains a tx minting a token into multiple assets within same box
    pub fn block_multi_asset_mint<'a>() -> BlockData<'a> {
        let tx_1_multi_asset_mint = Transaction {
            id: "0f778e5c5c2ecb8b26d2b7f739456e8942f3785ea1228ecfc0edda62e86b9177",
            index: 0,
            outputs: vec![
                Output {
                    box_id: String::from("e9ad4b744b96abc9244287b21c21720622f57b72d8fb2995c1fe4b4afe63f9d2"),
                    creation_height: 500114,
                    address: String::from("9hz1B19M44TNpmVe8MS4xvXyycehh5uP5aCfj4a6iAowj88hkd2"),
                    index: 0,
                    value: 1000000,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![
                        Asset {
                            token_id:
                            String::from("a342ae8776207b9a7529b93450187a33538ce86b68d11483758debffea667c25"),
                            amount: 10,
                        }, Asset {
                            token_id:
                            String::from("a342ae8776207b9a7529b93450187a33538ce86b68d11483758debffea667c25"),
                            amount: 10,
                        },
                    ],
                    size: 110,
                },
                Output {
                    box_id: String::from("9291258a91ccf04ed8e906484733d561cc3eaabdcb518426343e9b8d3a604660"),
                    creation_height: 500114,
                    address: String::from("2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"),
                    index: 1,
                    value: 1000000,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![],
                    size: 120,
                },
                Output {
                    box_id: String::from("e879169e8a393ae3f803e863bb4519983eea3ca0c5b6e8aa54cd25121a14ea9d"),
                    creation_height: 500114,
                    address: String::from("9hz1B19M44TNpmVe8MS4xvXyycehh5uP5aCfj4a6iAowj88hkd2"),
                    index: 2,
                    value: 16554330866,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![
                        Asset {
                            token_id:
                            String::from("2fc8abf612bc8b36af382e8c10a8e9df6227afdbe508c9b08b0a575fc4937b5e"),
                            amount: 100,
                        }, Asset {
                            token_id:
                            String::from("749fe0b8c63213be3451af2578eacabd620a9e687f5c55c54f1ec571b17c9c85"),
                            amount: 2,
                        }
                    ],
                    size: 130,
                },
            ],
            input_box_ids: vec![
                "a342ae8776207b9a7529b93450187a33538ce86b68d11483758debffea667c25",
                "78c70fb676d29e9ae9b90706cac55cd63d2e4519eaff5981291b56895511c929",
            ],
            data_input_box_ids: vec![],
        };
        BlockData {
            height: 500117,
            header_id: "bdecd56368e9c62ac51802b5cc5bb2446852227a7dd9448db817f9d5335c05ec",
            parent_header_id: "c2b3b5905965ebbf023dc092622e02301e711b346cf2459de9efe29c47c47ad5",
            timestamp: 1622328822475,
            votes: [0, 0, 0],
            transactions: vec![tx_1_multi_asset_mint],
        }
    }

    /// https://github.com/abchrisxyz/ergowatch/issues/27
    /// Reproducing issue where 2nd output of this tx causes a box_assets PK violation
    /// https://explorer.ergoplatform.com/en/transactions/467c9a4becd81354989fbc5101da03ca9fd407d7808b1269af9f793d8e65d3c9
    pub fn block_issue27<'a>() -> BlockData<'a> {
        let tx_1_multi_asset_mint = Transaction {
            id: "467c9a4becd81354989fbc5101da03ca9fd407d7808b1269af9f793d8e65d3c9",
            index: 0,
            outputs: vec![
                Output {
                    box_id: String::from("067d2db48bc674c277a2488293d58396b22bc04280542259fa4186abd42d0860"),
                    creation_height: 740228,
                    address: String::from("9h7abJG9Er7zqUp72PfboshWnqycXdkSZtahPrdi77TfWEJHmYR"),
                    index: 0,
                    value: 8705880108,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![
                        Asset {
                            token_id: String::from("a699d8e6467a9d0bb32d84c135b05dfb0cdddd4fc8e2caa9b9af0aa2666a3a6f"),
                            amount: 1500,
                        },
                        Asset {
                            token_id: String::from("0cd8c9f416e5b1ca9f986a7f10a84191dfb85941619e49e53c0dc30ebf83324b"),
                            amount: 1500,
                        },
                        Asset {
                            token_id: String::from("a699d8e6467a9d0bb32d84c135b05dfb0cdddd4fc8e2caa9b9af0aa2666a3a6f"),
                            amount: 3000,
                        },
                    ],
                    size: 110,
                },
            ],
            input_box_ids: vec![
                "b10c94a1c0452196307e62207b27f90b78d2249ee277f1bce722709ac4a4122f",
                "8bd6912ec5bdd8fb839dd53e84854e91f5e753acca831347adfba9b73a65f056",
            ],
            data_input_box_ids: vec![],
        };
        BlockData {
            height: 500117,
            header_id: "bdecd56368e9c62ac51802b5cc5bb2446852227a7dd9448db817f9d5335c05ec",
            parent_header_id: "c2b3b5905965ebbf023dc092622e02301e711b346cf2459de9efe29c47c47ad5",
            timestamp: 1622328822475,
            votes: [0, 0, 0],
            transactions: vec![tx_1_multi_asset_mint],
        }
    }
}

pub mod core;
pub mod sigma;
// pub mod oracle_pools;
// pub mod metrics;

use crate::node;

/// A preprocessed version of block data provided by the node.
///
/// Used as input to every processing unit.
///
/// - Decodes address bytes to strings.
/// - Casts unsigned ints to signed (for postgres compatibility)
pub struct BlockData<'a> {
    height: i32,
    header_id: &'a str,
    parent_header_id: &'a str,
    timestamp: i64,
    transactions: Vec<Transaction<'a>>,
}

impl<'a> BlockData<'a> {
    pub fn new(block: &'a node::models::Block) -> Self {
        BlockData {
            height: block.header.height as i32,
            header_id: &block.header.id,
            parent_header_id: &block.header.parent_id,
            timestamp: block.header.timestamp as i64,
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

struct Transaction<'a> {
    id: &'a str,
    index: i32,
    outputs: Vec<Output<'a>>,
    input_box_ids: Vec<&'a str>,
    data_input_box_ids: Vec<&'a str>,
}

impl<'a> Transaction<'a> {
    fn from_node_transaction(tx: &'a node::models::Transaction, index: usize) -> Self {
        Transaction {
            id: &tx.id,
            index: index as i32,
            outputs: tx
                .outputs
                .iter()
                .map(|op| Output::from_node_output(&op))
                .collect(),
            input_box_ids: tx.inputs.iter().map(|i| &*i.box_id).collect(),
            data_input_box_ids: tx.data_inputs.iter().map(|d| &*d.box_id).collect(),
        }
    }
}

struct Output<'a> {
    box_id: &'a str,
    creation_height: i32,
    address: String,
    index: i32,
    value: i64,
    additional_registers: [Option<Register>; 6],
    assets: Vec<Asset<'a>>,
}

impl<'a> Output<'a> {
    fn from_node_output(output: &'a node::models::Output) -> Self {
        Output {
            box_id: &output.box_id,
            creation_height: output.creation_height as i32,
            address: sigma::base16_to_address(&output.ergo_tree),
            index: output.index as i32,
            value: output.value as i64,
            additional_registers: parse_additional_registers(&output.additional_registers),
            assets: vec![],
        }
    }
}

impl Output<'_> {
    pub fn R4(&self) -> &Option<Register> {
        &self.additional_registers[0]
    }
    pub fn R5(&self) -> &Option<Register> {
        &self.additional_registers[1]
    }
    pub fn R6(&self) -> &Option<Register> {
        &self.additional_registers[2]
    }
    pub fn R7(&self) -> &Option<Register> {
        &self.additional_registers[3]
    }
    pub fn R8(&self) -> &Option<Register> {
        &self.additional_registers[4]
    }
    pub fn R9(&self) -> &Option<Register> {
        &self.additional_registers[5]
    }
}

#[derive(Debug)]
struct Register {
    stype: String,
    serialized_value: String,
    rendered_value: String,
}

fn parse_additional_registers(regs: &serde_json::Value) -> [Option<Register>; 6] {
    match regs {
        serde_json::Value::Null => [None, None, None, None, None, None],
        serde_json::Value::Object(map) => [
            match map.get("R4") {
                Some(v) => decode_register(&v),
                None => None,
            },
            match map.get("R5") {
                Some(v) => decode_register(&v),
                None => None,
            },
            match map.get("R6") {
                Some(v) => decode_register(&v),
                None => None,
            },
            match map.get("R7") {
                Some(v) => decode_register(&v),
                None => None,
            },
            match map.get("R8") {
                Some(v) => decode_register(&v),
                None => None,
            },
            match map.get("R9") {
                Some(v) => decode_register(&v),
                None => None,
            },
        ],
        _ => {
            panic!("Non map object for additional registers: {:?}", &regs);
        }
    }
}

fn decode_register(value: &serde_json::Value) -> Option<Register> {
    if let serde_json::Value::String(s) = value {
        let rendered_register = sigma::render_register_value(&s);
        return Some(Register {
            stype: rendered_register.value_type,
            serialized_value: String::new(),
            rendered_value: rendered_register.value,
        });
    }
    panic!("Non string value in register: {}", value);
}

struct Asset<'a> {
    token_id: &'a str,
    amount: i64,
}

#[cfg(test)]
mod tests {
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
        let node_output = &block_600k().block_transactions.transactions[1].outputs[0];
        let output = Output::from_node_output(&node_output);
        assert_eq!(output.box_id, node_output.box_id);
        assert_eq!(output.creation_height, node_output.creation_height as i32);
        assert_eq!(output.index, node_output.index as i32);
        assert_eq!(output.value, node_output.value as i64);
        assert_eq!(output.address, "jL2aaqw6XU61SZznvcri5VZnx1Gn8hfZWK87JH6PM7o1YMDMZfpH1uoGJSd3gDQabX6AmCZKLyMSBqSoUAo8X7E5oNRV9JgCdLBFjV6i1BEjZLwgGo3RUr4p8zchqrJ1FeGPLf2DidW6F41aeM1zCM64ZjfBqcy8d6fgEnAn53W28GEDQi5W1XCWRjFvgTFuDdAzd6Yj65KGJhdvMSgffP7pELpCtqK5Z4dX9SQKtt8Y4RMBaeEKtKB1pEx1n");
    }

    #[test]
    fn output_registers() {
        let node_output = &block_600k().block_transactions.transactions[1].outputs[0];
        let output = Output::from_node_output(&node_output);
        assert_eq!(
            &output.R4().as_ref().unwrap().rendered_value,
            "03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"
        );
        assert_eq!(
            &output.R5().as_ref().unwrap().rendered_value,
            "98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"
        );
        assert_eq!(
            &output.R6().as_ref().unwrap().rendered_value,
            "261824656027858"
        );
    }
}

#[cfg(test)]
mod testing {
    use super::Asset;
    use super::BlockData;
    use super::Output;
    use super::Register;
    use super::Transaction;
    pub fn block_600k<'a>() -> BlockData<'a> {
        let tx_1 = Transaction {
            id: "4ac89169a2f83adb895b3d76735dbcfc63ad7940bddc2492d9ee4201299bf927",
            index: 0,
            outputs: vec![Output {
                box_id: "029bc1cb151aaef51c3678d2c74f3e82c9f4d197dd37e7a4eb73612f9da4f1f6",
                creation_height: 600000,
                address: String::from("2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU"),
                index: 0,
                value: 52909132500000000,
                additional_registers: [None, None, None, None, None, None],
                assets: vec![],
            }, Output {
                box_id: "6cb8ffe391838b627cb893c9b2027aa2a03f3a20455dd11e5ac903c7e4179ace",
                creation_height: 600000,
                address: String::from("88dhgzEuTXaRvR2VKsnXYTGUPh3A9VK8ojeRcpHihcrBu23dnwbB12BbVcJuTcdGfRuSzA8bW25Az6n9"),
                index: 0,
                value: 67500000000,
                additional_registers: [None, None, None, None, None, None],
                assets: vec![],
            }],
            input_box_ids: vec!["eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f"],
            data_input_box_ids: vec![],
        };
        let tx_2 = Transaction {
            id: "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8",
            index: 1i32,
            outputs: vec![
                Output {
                    box_id: "aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d",
                    creation_height: 599998,
                    address: String::from("jL2aaqw6XU61SZznvcri5VZnx1Gn8hfZWK87JH6PM7o1YMDMZfpH1uoGJSd3gDQabX6AmCZKLyMSBqSoUAo8X7E5oNRV9JgCdLBFjV6i1BEjZLwgGo3RUr4p8zchqrJ1FeGPLf2DidW6F41aeM1zCM64ZjfBqcy8d6fgEnAn53W28GEDQi5W1XCWRjFvgTFuDdAzd6Yj65KGJhdvMSgffP7pELpCtqK5Z4dX9SQKtt8Y4RMBaeEKtKB1pEx1n"),
                    index: 0,
                    value: 1000000,
                    additional_registers: [
                        Some(Register {
                            stype: String::from("SLong"),
                            serialized_value: String::from("05a4c3edd9998877"),
                            rendered_value: String::from("261824656027858"),
                        }),
                        Some(Register {
                            stype: String::from("SGroupElement"),
                            serialized_value: String::from("0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"),
                            rendered_value: String::from("03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"),
                        }),
                        Some(Register {
                            stype: String::from("Coll[SByte]"),
                            serialized_value: String::from("0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"),
                            rendered_value: String::from("98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"),
                        }),
                        None, None, None],
                    assets: vec![
                        Asset {
                            token_id: "01e6498911823f4d36deaf49a964e883b2c4ae2a4530926f18b9c1411ab2a2c2",
                            amount: 1,
                        }
                    ],
                },
                Output {
                    box_id: "5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4",
                    creation_height: 599998,
                    address: String::from("2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"),
                    index: 1,
                    value: 1100000,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![],
                },
                Output {
                    box_id: "22adc6d1fd18e81da0ab9fa47bc389c5948780c98906c0ea3d812eba4ef17a33",
                    creation_height: 599998,
                    address: String::from("9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b"),
                    index: 1,
                    value: 2784172525,
                    additional_registers: [None, None, None, None, None, None],
                    assets: vec![],
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
                box_id: "98d0271b7a29d62b672d8dd002e38b8cfbfc8e4055a637422b3e9d59cd6ff86d",
                creation_height: 600000,
                address: String::from("2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"),
                index: 0,
                value: 1100000,
                additional_registers: [None, None, None, None, None, None],
                assets: vec![],
            }],
            input_box_ids: vec!["5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4"],
            data_input_box_ids: vec![],
        };
        BlockData {
            height: 600000,
            header_id: "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4",
            parent_header_id: "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1",
            timestamp: 1634511451404,
            transactions: vec![tx_1, tx_2, tx_3],
        }
    }
}

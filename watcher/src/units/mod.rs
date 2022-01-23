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

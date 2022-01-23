//! # core
//!
//! Process blocks into core tables data.

use super::BlockData;
use crate::db::core::data_inputs::DataInputRow;
use crate::db::core::header::HeaderRow;
use crate::db::core::inputs::InputRow;
use crate::db::core::outputs::OutputRow;
use crate::db::core::registers::BoxRegisterRow;
use crate::db::core::transaction::TransactionRow;
use crate::db::SQLStatement;

pub struct CoreUnit;

impl CoreUnit {
    pub fn prep(&self, block: &BlockData) -> Vec<SQLStatement> {
        let mut statements: Vec<SQLStatement> = vec![];
        statements.push(extract_header(&block));
        statements.append(&mut extract_transactions(&block));
        statements.append(&mut extract_outputs(&block));
        statements.append(&mut extract_inputs(&block));
        statements.append(&mut extract_data_inputs(&block));
        statements.append(&mut extract_additional_registers(&block));
        statements
    }

    // fn rollback(&self, block: &Block) -> () {
    //     let header = Header::from(block);
    //     db::core::delete_header(&header).unwrap();
    //     info!("Deleted header {} for height {}", header.id, header.height);
    // }
}
// Convert block header to sql statement
fn extract_header(block: &BlockData) -> SQLStatement {
    HeaderRow {
        height: block.height,
        id: block.header_id,
        parent_id: block.parent_header_id,
        timestamp: block.timestamp,
    }
    .to_statement()
}

// Convert block transactions to sql statements
fn extract_transactions(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .map(|tx| TransactionRow {
            id: &tx.id,
            header_id: &block.header_id,
            height: block.height,
            index: tx.index,
        })
        .map(|row| row.to_statement())
        .collect()
}

fn extract_outputs(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().map(|op| {
                OutputRow {
                    box_id: &op.box_id,
                    tx_id: &tx.id,
                    header_id: &block.header_id,
                    creation_height: op.creation_height,
                    address: &op.address,
                    index: op.index,
                    value: op.value,
                }
                .to_statement()
            })
        })
        .collect()
}

fn extract_inputs(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.input_box_ids.iter().enumerate().map(|(ix, id)| {
                InputRow {
                    box_id: &id,
                    tx_id: &tx.id,
                    header_id: &block.header_id,
                    index: ix as i32,
                }
                .to_statement()
            })
        })
        .collect()
}

fn extract_data_inputs(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.data_input_box_ids.iter().enumerate().map(|(ix, id)| {
                DataInputRow {
                    box_id: &id,
                    tx_id: &tx.id,
                    header_id: &block.header_id,
                    index: ix as i32,
                }
                .to_statement()
            })
        })
        .collect()
}

fn extract_additional_registers(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().flat_map(|op| {
                op.additional_registers
                    .iter()
                    .filter(|r| r.is_some())
                    .map(|r| r.as_ref().unwrap())
                    .map(|r| {
                        BoxRegisterRow {
                            id: r.id,
                            box_id: &op.box_id,
                            stype: &r.stype,
                            serialized_value: &r.serialized_value,
                            rendered_value: &r.rendered_value,
                        }
                        .to_statement()
                    })
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::CoreUnit;
    use crate::db;
    use crate::db::SQLArg;
    use crate::units::testing::block_600k;

    #[test]
    fn number_of_statements() -> () {
        let statements = CoreUnit.prep(&block_600k());
        // 1 header + 3 transactions + 6 outputs + 4 inputs + 1 data input + 3 registers
        assert_eq!(statements.len(), 18);
    }

    #[test]
    fn header_statement() -> () {
        let statements = CoreUnit.prep(&block_600k());
        let stmnt = &statements[0];
        assert_eq!(stmnt.sql, db::core::header::INSERT_HEADER);
        assert_eq!(stmnt.args[0], SQLArg::Integer(600000));
        assert_eq!(
            stmnt.args[1],
            SQLArg::Text(String::from(
                "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
            ))
        );
        assert_eq!(
            stmnt.args[2],
            SQLArg::Text(String::from(
                "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1"
            ))
        );
        assert_eq!(stmnt.args[3], SQLArg::BigInt(1634511451404));
    }

    #[test]
    fn transaction_statements() -> () {
        let statements = CoreUnit.prep(&block_600k());
        assert_eq!(statements[1].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[2].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[3].sql, db::core::transaction::INSERT_TRANSACTION);
    }

    #[test]
    fn output_statements() -> () {
        let statements = CoreUnit.prep(&block_600k());
        assert_eq!(statements[4].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[5].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[6].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[7].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[8].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[9].sql, db::core::outputs::INSERT_OUTPUT);
    }

    #[test]
    fn input_statements() -> () {
        let statements = CoreUnit.prep(&block_600k());
        assert_eq!(statements[10].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[11].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[12].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[13].sql, db::core::inputs::INSERT_INPUT);
    }

    #[test]
    fn data_input_statements() -> () {
        let statements = CoreUnit.prep(&block_600k());
        assert_eq!(statements[14].sql, db::core::data_inputs::INSERT_DATA_INPUT);
    }

    #[test]
    fn box_register_statements() -> () {
        let statements = CoreUnit.prep(&block_600k());
        assert_eq!(statements[15].sql, db::core::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[16].sql, db::core::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[17].sql, db::core::registers::INSERT_BOX_REGISTER);
    }
}

//! # core
//!
//! Process blocks into core tables data.

use super::BlockData;
use crate::db::core::header::HeaderRow;
use crate::db::core::outputs::OutputRow;
use crate::db::core::transaction::TransactionRow;
use crate::db::SQLStatement;

pub struct CoreUnit;

impl CoreUnit {
    pub fn prep(&self, block: &BlockData) -> Vec<SQLStatement> {
        let mut statements: Vec<SQLStatement> = vec![];
        statements.push(extract_header(&block));
        statements.append(&mut extract_transactions(&block));
        statements.append(&mut extract_outputs(&block));
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
// fn extract_header(block: &Block) -> SQLStatement {
//     HeaderRow {
//         height: block.header.height as i32,
//         id: &block.header.id,
//         parent_id: &block.header.parent_id,
//         timestamp: block.header.timestamp as i64,
//     }
//     .to_statement()
// }

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

#[cfg(test)]
mod tests {
    use super::CoreUnit;
    use crate::db;
    use crate::db::SQLArg;
    use crate::units::testing::block_600k;

    #[test]
    fn number_of_statements() -> () {
        let statements = CoreUnit.prep(&block_600k());
        // 1 header + 3 transactions + 6 outputs
        assert_eq!(statements.len(), 10);
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
}

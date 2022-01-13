//! # core
//!
//! Process blocks into core tables data.

use crate::db::core::HeaderRow;
use crate::db::SQLStatement;
use crate::node::models::Block;
// use crate::types::Transaction;

pub struct CoreUnit;

impl CoreUnit {
    pub fn prep(&self, block: &Block) -> Vec<SQLStatement> {
        let mut statements: Vec<SQLStatement> = vec![];
        let height = block.header.height;
        let header_id = &block.header.id;
        statements.push(
            HeaderRow {
                height: height as i32,
                id: header_id,
                parent_id: &block.header.parent_id,
                timestamp: block.header.timestamp as i64,
            }
            .to_statement(),
        );
        statements
    }

    // fn rollback(&self, block: &Block) -> () {
    //     let header = Header::from(block);
    //     db::core::delete_header(&header).unwrap();
    //     info!("Deleted header {} for height {}", header.id, header.height);
    // }
}

#[cfg(test)]
mod tests {
    use super::CoreUnit;
    use crate::db;
    use crate::db::SQLArg;
    use crate::node::models::{Block, BlockTransactions, Header};

    fn make_test_block() -> Block {
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
                transactions: vec![],
                block_version: 2,
                size: 1155,
            },
            size: 8486,
        }
    }

    #[test]
    fn number_of_statements() -> () {
        let statements = CoreUnit.prep(&make_test_block());
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn header_statement() -> () {
        let statements = CoreUnit.prep(&make_test_block());
        let stmnt = &statements[0];
        assert_eq!(stmnt.sql, db::core::INSERT_HEADER);
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
}

//! # core
//!
//! Process blocks into core tables data.

use crate::db;
use crate::db::core::CoreStatement;
use crate::db::core::{InsertHeaderStmt, InsertTransactionsStmt};
use crate::node::models::Block;
use crate::types::Header;
use crate::types::Transaction;

pub struct CoreUnit;

impl CoreUnit {
    pub fn prep(&self, block: &Block) -> Vec<db::Statement> {
        let mut statements: Vec<db::Statement> = vec![]; 
        let height = block.header.height;
        let header_id = &block.header.id;
        let statements = vec![
            db::Statement::Core(CoreStatement::InsertHeader(InsertHeaderStmt::new(Header::from(block)))),
            db::Statement::Core(CoreStatement::InsertTransactions(InsertTransactionsStmt::new(
                block.block_transactions.transactions
                    .iter()
                    .map(|t| Transaction {
                        id: String::from(&t.id),
                        header_id: String::from(header_id),
                        height: height,
                        index: 0, // TODOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO !!!!!
                    })
                    .collect()
            )))
        ];

        

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
    use crate::db;
    use crate::node::models::{Block, Header, BlockTransactions};
    use super::CoreUnit;

    fn make_test_block() -> Block {
        Block {
            header: Header {
                votes: String::from("000000"),
                timestamp: 1634511451404,
                size: 221,
                height: 600000,
                id: String::from("5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"),
                parent_id: String::from("eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1"),
            },
            block_transactions: BlockTransactions {
                header_id: String::from("5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"),
                transactions: vec![],
                block_version: 2,
                size: 1155,
            },
            size: 8486,
        }
    }
    #[test]
    fn init_works() -> () {
        let block = make_test_block();
        let unit = CoreUnit;
        let statements: Vec<db::Statement> = unit.prep(&block);
        assert_eq!(statements.len(), 2);
        match &statements[0] {
            db::Statement::Core(db::core::CoreStatement::InsertHeader(stmt)) => {
                assert_eq!(stmt.header, crate::types::Header::from(block));
            },
            _ => panic!()
        }
    }
}
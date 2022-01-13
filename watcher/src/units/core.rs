//! # core
//!
//! Process blocks into core tables data.

use crate::db::core::header::HeaderRow;
use crate::db::core::transaction::TransactionRow;
use crate::db::SQLStatement;
use crate::node::models::Block;
// use crate::types::Transaction;

pub struct CoreUnit;

impl CoreUnit {
    pub fn prep(&self, block: &Block) -> Vec<SQLStatement> {
        let mut statements: Vec<SQLStatement> = vec![];
        let height = block.header.height as i32;
        let header_id = &block.header.id;
        // Header
        statements.push(
            HeaderRow {
                height: height,
                id: header_id,
                parent_id: &block.header.parent_id,
                timestamp: block.header.timestamp as i64,
            }
            .to_statement(),
        );
        // Transactions
        statements.append(
            &mut block
                .block_transactions
                .transactions
                .iter()
                .enumerate()
                .map(|(i, tx)| TransactionRow {
                    id: &tx.id,
                    header_id: &header_id,
                    height: height,
                    index: i as i32,
                })
                .map(|row| row.to_statement())
                .collect(),
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
    use crate::node::models::testing::block_600k;

    // fn make_test_block() -> Block {
    //     Block {
    //         header: Header {
    //             votes: String::from("000000"),
    //             timestamp: 1634511451404,
    //             size: 221,
    //             height: 600000,
    //             id: String::from(
    //                 "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4",
    //             ),
    //             parent_id: String::from(
    //                 "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1",
    //             ),
    //         },
    //         block_transactions: BlockTransactions {
    //             header_id: String::from(
    //                 "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4",
    //             ),
    //             transactions: vec![
    //                 Transaction {
    //                     id: String::from(
    //                         "4ac89169a2f83adb895b3d76735dbcfc63ad7940bddc2492d9ee4201299bf927",
    //                     ),
    //                     inputs: vec![],
    //                     data_inputs: vec![],
    //                     outputs: vec![],
    //                     size: 344,
    //                 },
    //                 Transaction {
    //                     id: String::from(
    //                         "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8",
    //                     ),
    //                     inputs: vec![],
    //                     data_inputs: vec![],
    //                     outputs: vec![],
    //                     size: 674,
    //                 },
    //             ],
    //             block_version: 2,
    //             size: 1155,
    //         },
    //         size: 8486,
    //     }
    // }

    #[test]
    fn number_of_statements() -> () {
        let statements = CoreUnit.prep(&block_600k());
        // 1 header + 3 transactions
        assert_eq!(statements.len(), 4);
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
    fn transaction_statement() -> () {
        let statements = CoreUnit.prep(&block_600k());
        assert_eq!(statements[1].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[2].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[3].sql, db::core::transaction::INSERT_TRANSACTION);
        // let stmnt = &statements[1];
    }
}

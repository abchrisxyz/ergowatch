//! # core
//!
//! Read/write access to core tables.

use super::SQLArg;
use super::SQLStatement;

pub const INSERT_HEADER: &str = "\
    insert into core.headers (height, id, parent_id, timestamp) \
    values ($1, $2, $3, $4);";

pub struct HeaderRow<'a> {
    pub height: i32,
    pub id: &'a str,
    pub parent_id: &'a str,
    pub timestamp: i64,
}

impl HeaderRow<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_HEADER),
            args: vec![
                SQLArg::Integer(self.height),
                SQLArg::Text(String::from(self.id)),
                SQLArg::Text(String::from(self.parent_id)),
                SQLArg::BigInt(self.timestamp),
            ],
        }
    }
}

// pub fn make_stmt_insert_header<'a>(
//     row: HeaderRow,
// ) -> super::SQLStatement<'a> {
//     super::SQLStatement {
//         sql: String::from(INSERT_HEADER),
//         args: vec![
//             super::SQLArg::Integer(height),
//             super::SQLArg::Text(id),
//             super::SQLArg::Text(parent_id),
//             super::SQLArg::BigInt(timestamp),
//         ],
//     }
// }

// pub struct InsertTransactionsStmt {
//     transactions: Vec<Transaction>,
// }

// impl InsertTransactionsStmt {
//     pub fn new(transactions: Vec<Transaction>) -> Self {
//         Self {
//             transactions: transactions,
//         }
//     }

//     pub fn execute(&self, tx: &mut postgres::Transaction) -> Result<(), postgres::Error> {
//         for t in &self.transactions {
//             let height: i32 = t.height as i32;
//             tx.execute(
//                 "insert into core.transactions (id, header_id, height, index) values ($1, $2, $3, $4);",
//                 &[&t.id, &t.header_id, &height, &(t.index as i32)],
//             )?;
//         }
//         Ok(())
//     }

//     pub fn to_sql(&self) -> String {
//         todo!()
//         // format!(
//         //     "insert into core.transactions (id, header_id, height, index) values ('{}', '{}', {}, {});",
//         //     "dummy", "dummy", 0, 0
//         // )
//     }
// }

// pub fn get_height() -> Result<u32, postgres::Error> {
//     debug!("Retrieving sync height from db");
//     let mut client = Client::connect(
//         "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
//         NoTls,
//     )?;
//     let row = client.query_one(
//         "select 0 as height union select height from core.headers order by 1 desc limit 1;",
//         &[],
//     )?;
//     let height: i32 = row.get("height");
//     Ok(height as u32)
// }

// pub fn insert_header(header: &Header) -> Result<(), postgres::Error> {
//     let mut client = Client::connect(
//         "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
//         NoTls,
//     )?;
//     let height: i32 = header.height as i32;
//     let timestamp: i64 = header.timestamp as i64;
//     client.execute(
//         "insert into core.headers (height, id, parent_id, timestamp) values ($1, $2, $3, $4);",
//         &[&height, &header.id, &header.parent_id, &timestamp],
//     )?;
//     Ok(())
// }

// pub fn delete_header(header: &Header) -> Result<(), postgres::Error> {
//     let mut client = Client::connect(
//         "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
//         NoTls,
//     )?;
//     let deleted = client.execute("delete from core.headers where id = $1;", &[&header.id])?;
//     assert_eq!(deleted, 1);
//     Ok(())
// }

// pub fn get_last_header() -> Result<Option<crate::types::Header>, postgres::Error> {
//     let mut client = Client::connect(
//         "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
//         NoTls,
//     )?;
//     let row_opt = client.query_opt(
//         "select height \
//             , id \
//             , parent_id \
//             , timestamp \
//          from core.headers \
//          order by 1 desc \
//          limit 1;",
//         &[],
//     )?;
//     match row_opt {
//         Some(row) => {
//             let height: i32 = row.get("height");
//             let timestamp: i64 = row.get("timestamp");
//             let header = crate::types::Header {
//                 height: height as u32,
//                 id: row.get("id"),
//                 parent_id: row.get("parent_id"),
//                 timestamp: timestamp as u64,
//             };
//             Ok(Some(header))
//         },
//         None => Ok(None)
//     }
// }

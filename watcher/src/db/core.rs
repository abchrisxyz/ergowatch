//! # core
//!
//! Read/write access to core tables.

use log::debug;

use crate::types::Header;
use crate::types::Transaction;


pub enum CoreStatement {
    InsertHeader(InsertHeaderStmt),
    InsertTransactions(InsertTransactionsStmt),
}

impl CoreStatement {
    pub fn execute(&self, tx: &mut postgres::Transaction) -> Result<(), postgres::Error> {
        match self {
            Self::InsertHeader(stmt) => stmt.execute(tx),
            Self::InsertTransactions(stmt) => stmt.execute(tx),
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            Self::InsertHeader(stmt) => stmt.to_sql(),
            Self::InsertTransactions(stmt) => stmt.to_sql(),
        }
    }
}

pub struct InsertHeaderStmt {
    pub header: Header,
}

impl InsertHeaderStmt {
    pub fn new(header: Header) -> Self {
        Self {header: header}
    }

    pub fn execute(&self, tx: &mut postgres::Transaction) -> Result<(), postgres::Error> {
        let height: i32 = self.header.height as i32;
        let timestamp: i64 = self.header.timestamp as i64;
        tx.execute(
            "insert into core.headers (height, id, parent_id, timestamp) values ($1, $2, $3, $4);",
            &[&height, &self.header.id, &self.header.parent_id, &timestamp],
        )?;
        debug!("Added header {} for height {}", self.header.id, self.header.height);
        Ok(())
    }

    pub fn to_sql(&self) -> String {
        format!(
            "insert into core.headers (height, id, parent_id, timestamp) values ({}, '{}', '{}', {});",
            self.header.height, self.header.id, self.header.parent_id, self.header.timestamp
        )
    }
}

pub struct InsertTransactionsStmt {
    transactions: Vec<Transaction>
}

impl InsertTransactionsStmt {
    pub fn new(transactions: Vec<Transaction>) -> Self {
        Self {transactions: transactions}
    }

    pub fn execute(&self, tx: &mut postgres::Transaction) -> Result<(), postgres::Error> {
        for t in &self.transactions {
            let height: i32 = t.height as i32;
            tx.execute(
                "insert into core.transactions (id, header_id, height, index) values ($1, $2, $3, $4);",
                &[&t.id, &t.header_id, &height, &(t.index as i32)],
            )?;
        }
        Ok(())
    }

    pub fn to_sql(&self) -> String {
        todo!()
        // format!(
        //     "insert into core.transactions (id, header_id, height, index) values ('{}', '{}', {}, {});",
        //     "dummy", "dummy", 0, 0
        // )
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn insert_header_statement_to_sql() {
        let header = crate::types::Header {
             height: 8,
             id: String::from("dummy_id"),
             parent_id: String::from("dummy_parent_id"),
             timestamp: 123456789,
        };
        let stmt = super::InsertHeaderStmt::new(header);
        assert_eq!(stmt.to_sql(), "\
            insert into core.headers (height, id, parent_id, timestamp) \
            values (8, 'dummy_id', 'dummy_parent_id', 123456789);"
        )
    }
}

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

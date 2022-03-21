use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_TRANSACTION: &str = "\
insert into core.transactions (id, header_id, height, index) \
    values ($1, $2, $3, $4);";

pub struct TransactionRow<'a> {
    pub id: &'a str,
    pub header_id: &'a str,
    pub height: i32,
    pub index: i32,
}

impl TransactionRow<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_TRANSACTION),
            args: vec![
                SQLArg::Text(String::from(self.id)),
                SQLArg::Text(String::from(self.header_id)),
                SQLArg::Integer(self.height),
                SQLArg::Integer(self.index),
            ],
        }
    }
}

pub mod constraints {
    pub const ADD_PK: &str = "alter table core.transactions add primary key (id);";
    pub const FK_HEADER_ID: &str = "alter table core.transactions add foreign key (header_id)
        references core.headers (id) on delete cascade;";
    pub const IDX_HEIGHT: &str = "create index on core.transactions(height);";
}

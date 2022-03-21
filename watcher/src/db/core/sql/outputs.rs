use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_OUTPUT: &str = "\
    insert into core.outputs (box_id, tx_id, header_id, creation_height, address, index, value) \
    values ($1, $2, $3, $4, $5, $6, $7);";

pub struct OutputRow<'a> {
    pub box_id: &'a str,
    pub tx_id: &'a str,
    pub header_id: &'a str,
    pub creation_height: i32,
    pub address: &'a str,
    pub index: i32,
    pub value: i64,
    // pub additional_registers: &'a serde_json::Value,
}

impl OutputRow<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_OUTPUT),
            args: vec![
                SQLArg::Text(String::from(self.box_id)),
                SQLArg::Text(String::from(self.tx_id)),
                SQLArg::Text(String::from(self.header_id)),
                SQLArg::Integer(self.creation_height),
                SQLArg::Text(String::from(self.address)),
                SQLArg::Integer(self.index),
                SQLArg::BigInt(self.value),
                // SQLArg::Json(self.additional_registers.clone()),
            ],
        }
    }
}

pub mod constraints {
    pub const ADD_PK: &str = "alter table core.outputs add primary key (box_id);";
    pub const NOT_NULL_TX_ID: &str = "alter table core.outputs alter column tx_id set not null;";
    pub const NOT_NULL_HEADER_ID: &str =
        "alter table core.outputs alter column header_id set not null;";
    pub const NOT_NULL_ADDRESS: &str =
        "alter table core.outputs alter column address set not null;";
    pub const FK_TX_ID: &str = "alter table core.outputs add foreign key (tx_id)
        references core.transactions (id) on delete cascade;";
    pub const FK_HEADER_ID: &str = "alter table core.outputs add foreign key (header_id)
        references core.headers (id) on delete cascade;";
    pub const IDX_TX_ID: &str = "create index on core.outputs(tx_id);";
    pub const IDX_HEADER_ID: &str = "create index on core.outputs(header_id);";
    pub const IDX_ADDRESS: &str = "create index on core.outputs(address);";
    pub const IDX_INDEX: &str = "create index on core.outputs(index);";
}

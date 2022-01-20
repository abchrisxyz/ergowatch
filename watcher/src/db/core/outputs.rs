use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_OUTPUT: &str = "\
    insert into core.outputs (box_id, tx_id, header_id, creation_height, address, index, value, additional_registers) \
    values ($1, $2, $3, $4, $5, $6, $7, $8);";

pub struct OutputRow<'a> {
    pub box_id: &'a str,
    pub tx_id: &'a str,
    pub header_id: &'a str,
    pub creation_height: i32,
    pub address: &'a str,
    pub index: i32,
    pub value: i64,
    pub additional_registers: &'a serde_json::Value,
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
                SQLArg::Json(self.additional_registers.clone()),
            ],
        }
    }
}

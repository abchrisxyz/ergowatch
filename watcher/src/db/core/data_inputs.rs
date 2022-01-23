use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_DATA_INPUT: &str = "\
    insert into core.data_inputs (box_id, tx_id, header_id, index) \
    values ($1, $2, $3, $4);";

pub struct DataInputRow<'a> {
    pub box_id: &'a str,
    pub tx_id: &'a str,
    pub header_id: &'a str,
    pub index: i32,
}

impl DataInputRow<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_DATA_NPUT),
            args: vec![
                SQLArg::Text(String::from(self.box_id)),
                SQLArg::Text(String::from(self.tx_id)),
                SQLArg::Text(String::from(self.header_id)),
                SQLArg::Integer(self.index),
            ],
        }
    }
}

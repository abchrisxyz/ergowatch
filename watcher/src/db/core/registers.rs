use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_BOX_REGISTER: &str = "\
    insert into core.box_registers (id, box_id, tx_id, header_id, index) \
    values ($1, $2, $3, $4);";

pub struct BoxRegisterRow<'a> {
    pub id: i16,
    pub box_id: &'a str,
    pub serialized: &'a str,
    pub rendered: &'a str,
}

impl BoxRegisterRow<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_BOX_REGISTER),
            args: vec![
                SQLArg::SmallInt(self.id),
                SQLArg::Text(String::from(self.box_id)),
                SQLArg::Text(String::from(self.serialized)),
                SQLArg::Text(String::from(self.rendered)),
            ],
        }
    }
}

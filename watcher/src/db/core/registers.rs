use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_BOX_REGISTER: &str = "\
    insert into core.box_registers (id, box_id, stype, serialized_value, rendered_value) \
    values ($1, $2, $3, $4 &5);";

pub struct BoxRegisterRow<'a> {
    pub id: i16,
    pub box_id: &'a str,
    pub stype: &'a str,
    pub serialized_value: &'a str,
    pub rendered_value: &'a str,
}

impl BoxRegisterRow<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_BOX_REGISTER),
            args: vec![
                SQLArg::SmallInt(self.id),
                SQLArg::Text(String::from(self.box_id)),
                SQLArg::Text(String::from(self.stype)),
                SQLArg::Text(String::from(self.serialized_value)),
                SQLArg::Text(String::from(self.rendered_value)),
            ],
        }
    }
}

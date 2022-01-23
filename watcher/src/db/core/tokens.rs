use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_TOKEN: &str = "\
    insert into core.tokens (token_id, box_id, emission_amount, name, description, type, decimals) \
    values ($1, $2, $3, $4, $5, $6, $7);";

pub struct BoxAssetRow<'a> {
    pub token_id: &'a str,
    pub box_id: &'a str,
    pub emission_amount: i64,
    pub name: &'a str,
    pub description: &'a str,
    pub token_type: &'a str,
    pub decimals: i32,
}

impl InputRow<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_TOKEN),
            args: vec![
                SQLArg::Text(String::from(self.token_id)),
                SQLArg::Text(String::from(self.box_id)),
                SQLArg::BigInt(String::from(self.emission_amount)),
                SQLArg::Text(String::from(self.name)),
                SQLArg::Text(String::from(self.description)),
                SQLArg::Text(String::from(self.token_type)),
                SQLArg::Integer(self.value),
            ],
        }
    }
}

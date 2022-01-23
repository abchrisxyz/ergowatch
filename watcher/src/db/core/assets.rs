use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_BOX_ASSET: &str = "\
    insert into core.box_assets (box_id, token_id, amount) \
    values ($1, $2, $3);";

pub struct BoxAssetRow<'a> {
    pub box_id: &'a str,
    pub token_id: &'a str,
    pub amount: i64,
}

impl InputRow<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_BOX_ASSET),
            args: vec![
                SQLArg::Text(String::from(self.box_id)),
                SQLArg::Text(String::from(self.token_id)),
                SQLArg::BigInt(self.amount),
            ],
        }
    }
}

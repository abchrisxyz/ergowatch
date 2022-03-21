use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_TOKEN: &str = "\
    insert into core.tokens (id, box_id, emission_amount) \
    values ($1, $2, $3);";

pub const INSERT_TOKEN_EIP4: &str = "\
    insert into core.tokens (id, box_id, emission_amount, name, description, decimals, standard) \
    values ($1, $2, $3, $4, $5, $6, $7);";

const TOKEN_STANDARD_EIP4: &str = "EIP-004";

pub struct TokenRow<'a> {
    pub token_id: &'a str,
    pub box_id: &'a str,
    pub emission_amount: i64,
}

impl TokenRow<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_TOKEN),
            args: vec![
                SQLArg::Text(String::from(self.token_id)),
                SQLArg::Text(String::from(self.box_id)),
                SQLArg::BigInt(self.emission_amount),
            ],
        }
    }
}

pub struct TokenRowEIP4<'a> {
    pub token_id: &'a str,
    pub box_id: &'a str,
    pub emission_amount: i64,
    pub name: String,
    pub description: String,
    pub decimals: i32,
}

impl TokenRowEIP4<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_TOKEN_EIP4),
            args: vec![
                SQLArg::Text(String::from(self.token_id)),
                SQLArg::Text(String::from(self.box_id)),
                SQLArg::BigInt(self.emission_amount),
                SQLArg::Text(String::from(&self.name)),
                SQLArg::Text(String::from(&self.description)),
                SQLArg::Integer(self.decimals),
                SQLArg::Text(String::from(TOKEN_STANDARD_EIP4)),
            ],
        }
    }
}

pub mod constraints {
    pub const ADD_PK: &str = "alter table core.tokens add primary key (id, box_id);";
    pub const NOT_NULL_BOX_ID: &str = "alter table core.tokens alter column box_id set not null;";
    pub const FK_BOX_ID: &str = "alter table core.tokens	add foreign key (box_id)
        references core.outputs (box_id)
        on delete cascade;";
    pub const CHECK_EMISSION_AMOUNT_GT0: &str =
        "alter table core.tokens add check (emission_amount > 0);";
}

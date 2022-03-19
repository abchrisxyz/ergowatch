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
            sql: String::from(INSERT_DATA_INPUT),
            args: vec![
                SQLArg::Text(String::from(self.box_id)),
                SQLArg::Text(String::from(self.tx_id)),
                SQLArg::Text(String::from(self.header_id)),
                SQLArg::Integer(self.index),
            ],
        }
    }
}

pub mod constraints {
    pub const ADD_PK: &str = "alter table core.data_inputs add primary key (box_id, tx_id);";
    pub const NOT_NULL_HEADER_ID: &str =
        "alter table core.data_inputs alter column header_id set not null;";
    pub const FK_TX_ID: &str = "alter table core.data_inputs add foreign key (tx_id)
        references core.transactions (id)
        on delete cascade;";
    pub const FK_HEADER_ID: &str = "alter table core.data_inputs add foreign key (header_id)
        references core.headers (id)
        on delete cascade;";
    pub const FK_BOX_ID: &str = "alter table core.data_inputs add foreign key (box_id)
        references core.outputs (box_id)
        on delete cascade;";
    pub const IDX_TX_ID: &str = "create index on core.data_inputs(tx_id);";
    pub const IDX_HEADER_ID: &str = "create index on core.data_inputs(header_id);";
}

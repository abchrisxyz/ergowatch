//! # usp schema
//!
//! Maintains set of unspent boxes
use crate::db;

pub const DELETE_SPENT_BOX: &str = "delete from usp.boxes where box_id = $1;";

pub const INSERT_NEW_BOX: &str = "insert into usp.boxes (box_id) values ($1);";

pub const TRUNCATE_UNSPENT_BOXES: &str = "truncate usp.boxes;";

// Find all unspent boxes: outputs not used as input
pub const BOOTSTRAP_UNSPENT_BOXES: &str = "
    with inputs as (
        select ip.box_id
        from core.inputs ip
        join core.headers hs on hs.id = ip.header_id
    )
    insert into usp.boxes (box_id)
    select op.box_id
    from core.outputs op
    join core.headers hs on hs.id = op.header_id
    left join inputs ip on ip.box_id = op.box_id
    where ip.box_id is null;";

pub fn delete_spent_box_statement(box_id: &str) -> db::SQLStatement {
    db::SQLStatement {
        sql: String::from(DELETE_SPENT_BOX),
        args: vec![db::SQLArg::Text(String::from(box_id))],
    }
}

pub fn insert_new_box_statement(box_id: &str) -> db::SQLStatement {
    db::SQLStatement {
        sql: String::from(INSERT_NEW_BOX),
        args: vec![db::SQLArg::Text(String::from(box_id))],
    }
}

pub fn truncate_statement() -> db::SQLStatement {
    db::SQLStatement {
        sql: String::from(TRUNCATE_UNSPENT_BOXES),
        args: vec![],
    }
}

pub fn bootstrap_statement() -> db::SQLStatement {
    db::SQLStatement {
        sql: String::from(BOOTSTRAP_UNSPENT_BOXES),
        args: vec![],
    }
}

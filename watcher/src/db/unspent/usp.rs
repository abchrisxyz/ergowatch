//! # usp schema
//!
//! Maintains set of unspent boxes
use crate::db;

pub const DELETE_SPENT_BOX: &str = "delete from usp.boxes where box_id = $1;";

pub const INSERT_NEW_BOX: &str = "insert into usp.boxes (box_id) values ($1);";

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

pub mod bootstrapping {
    use crate::db::SQLArg;
    use crate::db::SQLStatement;

    // Find all unspent boxes: outputs not used as input
    pub const INSERT_NEW_BOXES_AT_HEIGHT: &str = "
    with inputs as (
        select ip.box_id
        from core.inputs ip
        join core.headers hs on hs.id = ip.header_id
        where hs.height = $1
    )
    insert into usp.boxes (box_id)
    select op.box_id
    from core.outputs op
    join core.headers hs on hs.id = op.header_id
    left join inputs ip on ip.box_id = op.box_id
    where hs.height = $1
        and ip.box_id is null;";

    pub fn insert_new_boxes_statement(height: i32) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_NEW_BOXES_AT_HEIGHT),
            args: vec![SQLArg::Integer(height)],
        }
    }

    pub const DELETE_SPENT_BOXES_AT_HEIGHT: &str = "
    delete from usp.boxes bx
    using core.inputs ip, core.headers hs
    where ip.header_id = hs.id
        and bx.box_id = ip.box_id
        and hs.height = $1;";

    pub fn delete_spent_boxes_statement(height: i32) -> SQLStatement {
        SQLStatement {
            sql: String::from(DELETE_SPENT_BOXES_AT_HEIGHT),
            args: vec![SQLArg::Integer(height)],
        }
    }
}

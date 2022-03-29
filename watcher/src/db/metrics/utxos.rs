use crate::db::SQLArg;
use crate::db::SQLStatement;

// Add a new snapshot record
pub const INSERT_SNAPSHOT: &str = "insert into mtr.utxos (height, value) values ($1, $2);";

pub fn insert_snapshot(height: i32, utxos: i64) -> SQLStatement {
    SQLStatement {
        sql: String::from(INSERT_SNAPSHOT),
        args: vec![SQLArg::Integer(height), SQLArg::BigInt(utxos)],
    }
}

// Delete snapshot at given height
pub const DELETE_SNAPSHOT: &str = "delete from mtr.utxos where height = $1;";

pub fn delete_snapshot(height: i32) -> SQLStatement {
    SQLStatement {
        sql: String::from(DELETE_SNAPSHOT),
        args: vec![SQLArg::Integer(height)],
    }
}

// UTXO count boostrapping.
// Relies on previous height being boostrapped.
pub const APPEND_SNAPSHOT_FROM_HEIGHT: &str = "
    insert into mtr.utxos (height, value)
    select $1
    , (
        select value
        from mtr.utxos
        where height = $1 - 1
    ) + (
        select count(*)
        from core.outputs op
        join core.headers hs on hs.id = op.header_id 
        where hs.height = $1
    ) - (
        select count(*)
        from core.inputs op
        join core.headers hs on hs.id = op.header_id 
        where hs.height = $1
    )
";

pub fn append_snapshot_from_height(height: i32) -> SQLStatement {
    SQLStatement {
        sql: String::from(APPEND_SNAPSHOT_FROM_HEIGHT),
        args: vec![SQLArg::Integer(height)],
    }
}

// Genesis UTXO count
pub const INSERT_GENESIS_SNAPSHOT: &str = "
    insert into mtr.utxos (height, value)
    select 0
    , (
        select count(*)
        from core.outputs op
        join core.headers hs on hs.id = op.header_id 
        where hs.height = 0
    )
";

pub fn insert_genesis_snapshot() -> SQLStatement {
    SQLStatement {
        sql: String::from(INSERT_GENESIS_SNAPSHOT),
        args: vec![],
    }
}

// Cache loading
pub const SELECT_LAST_SNAPSHOT_VALUE: &str =
    "select value from mtr.utxos order by height desc limit 1";

pub mod constraints {
    pub const ADD_PK: &str = "alter table mtr.utxos add primary key(height);";
}

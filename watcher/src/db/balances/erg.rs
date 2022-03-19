use crate::db::SQLArg;
use crate::db::SQLStatement;

// Updates balances for known addresses
pub const UPDATE_BALANCES: &str = "
    with new_diffs as (
        select address
            , sum(value) as value
        from bal.erg_diffs
        where height = $1
        group by 1
    )
    update bal.erg b
    set value = b.value + d.value
    from new_diffs d
    where d.address = b.address;";

pub fn update_statement(height: i32) -> SQLStatement {
    SQLStatement {
        sql: String::from(UPDATE_BALANCES),
        args: vec![SQLArg::Integer(height)],
    }
}

// Inserts balances for new addresses
pub const INSERT_BALANCES: &str = "
    with new_addresses as (
        select d.address
            , sum(d.value) as value
        from bal.erg_diffs d
        left join bal.erg b on b.address = d.address
        where d.height = $1
            and b.address is null
        group by 1
    )
    insert into bal.erg(address, value)
    select address
        , value
    from new_addresses;";

pub fn insert_statement(height: i32) -> SQLStatement {
    SQLStatement {
        sql: String::from(INSERT_BALANCES),
        args: vec![SQLArg::Integer(height)],
    }
}

// Undo balance updates
pub const ROLLBACK_BALANCE_UPDATES: &str = "
    with new_diffs as (
        select address
            , sum(value) as value
        from bal.erg_diffs
        where height = $1
        group by 1
    )
    update bal.erg b
    set value = b.value - d.value
    from new_diffs d
    where d.address = b.address;";

pub fn rollback_update_statement(height: i32) -> SQLStatement {
    SQLStatement {
        sql: String::from(ROLLBACK_BALANCE_UPDATES),
        args: vec![SQLArg::Integer(height)],
    }
}

pub const ROLLBACK_DELETE_ZERO_BALANCES: &str = "
    with deleted_addresses as (
        select d.address
            , sum(d.value) as value
        from bal.erg_diffs d
        left join bal.erg b on b.address = d.address
        where d.height = $1
            and b.address is null
        group by 1
    )
    insert into bal.erg(address, value)
    select address
        , 0 -- actual value will be set by update rollback
    from deleted_addresses;";

pub fn rollback_delete_zero_balances_statement(height: i32) -> SQLStatement {
    SQLStatement {
        sql: String::from(ROLLBACK_DELETE_ZERO_BALANCES),
        args: vec![SQLArg::Integer(height)],
    }
}

pub const DELETE_ZERO_BALANCES: &str = "
    delete from bal.erg
    where value = 0;";

pub fn delete_zero_balances_statement() -> SQLStatement {
    SQLStatement {
        sql: String::from(DELETE_ZERO_BALANCES),
        args: vec![],
    }
}

pub mod constraints {
    pub const ADD_PK: &str = "alter table bal.erg add primary key(address);";
    pub const CHECK_VALUE_GE0: &str = "alter table bal.erg add check (value >= 0);";
    pub const IDX_VALUE: &str = "create index on bal.erg(value);";
}

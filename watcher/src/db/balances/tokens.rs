use crate::db::SQLArg;
use crate::db::SQLStatement;

// Updates balances for known addresses
pub const UPDATE_BALANCES: &str = "
    with new_diffs as (
        select address
            , token_id
            , sum(value) as value
        from bal.tokens_diffs
        where height = $1
        group by 1, 2
    )
    update bal.tokens b
    set value = b.value + d.value
    from new_diffs d
    where d.address = b.address
        and d.token_id = b.token_id;";

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
            , d.token_id
            , sum(d.value) as value
        from bal.tokens_diffs d
        left join bal.tokens b
            on b.address = d.address
            and b.token_id = d.token_id
        where d.height = $1
            and b.address is null
        group by 1, 2
    )
    insert into bal.tokens(address, token_id, value)
    select address
        , token_id
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
            , token_id
            , sum(value) as value
        from bal.tokens_diffs
        where height = $1
        group by 1, 2
    )
    update bal.tokens b
    set value = b.value - d.value
    from new_diffs d
    where d.address = b.address
        and d.token_id = b.token_id;";

pub fn rollback_update_statement(height: i32) -> SQLStatement {
    SQLStatement {
        sql: String::from(ROLLBACK_BALANCE_UPDATES),
        args: vec![SQLArg::Integer(height)],
    }
}

pub const ROLLBACK_DELETE_ZERO_BALANCES: &str = "
    with deleted_addresses as (
        select d.address
            , d.token_id
            , sum(d.value) as value
        from bal.tokens_diffs d
        left join bal.tokens b
            on b.address = d.address
            and b.token_id = d.token_id
        where d.height = $1
            and b.address is null
        group by 1, 2
    )
    insert into bal.tokens(address, token_id, value)
    select address
        , token_id
        , 0 -- actual value will be set by update rollback
    from deleted_addresses;";

pub fn rollback_delete_zero_balances_statement(height: i32) -> SQLStatement {
    SQLStatement {
        sql: String::from(ROLLBACK_DELETE_ZERO_BALANCES),
        args: vec![SQLArg::Integer(height)],
    }
}

pub const DELETE_ZERO_BALANCES: &str = "
    delete from bal.tokens
    where value = 0;";

pub fn delete_zero_balances_statement() -> SQLStatement {
    SQLStatement {
        sql: String::from(DELETE_ZERO_BALANCES),
        args: vec![],
    }
}

pub mod constraints {
    pub const ADD_PK: &str = "alter table bal.tokens add primary key(address, token_id);";
    pub const CHECK_VALUE_GE0: &str = "alter table bal.tokens add check (value >= 0);";
    pub const IDX_VALUE: &str = "create index on bal.tokens(value);";
}

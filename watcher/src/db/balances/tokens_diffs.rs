use crate::db::SQLArg;
use crate::db::SQLStatement;

pub const INSERT_DIFFS: &str = "
    with inputs as (
        select tx.height
            , tx.id as tx_id
            , op.address
            , ba.token_id
            , sum(ba.amount) as value
        from core.transactions tx
        join core.inputs ip on ip.tx_id = tx.id
        join core.outputs op on op.box_id = ip.box_id
        join core.box_assets ba on ba.box_id = ip.box_id
        where tx.id = $1
        group by 1, 2, 3, 4
    ), outputs as (
        select tx.height
            , tx.id as tx_id
            , op.address
            , ba.token_id
            , sum(ba.amount) as value
        from core.transactions tx
        join core.outputs op on op.tx_id = tx.id
        join core.box_assets ba on ba.box_id = op.box_id
        where tx.id = $1
        group by 1, 2, 3, 4
    )
    insert into bal.tokens_diffs (address, token_id, height, tx_id, value)
    select coalesce(i.address, o.address) as address
        , coalesce(i.token_id, o.token_id) as token_id
        , coalesce(i.height, o.height) as height
        , coalesce(i.tx_id, o.tx_id) as tx_id
        , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0))
    from inputs i
    full outer join outputs o
        on o.address = i.address
        and o.token_id = i.token_id
    group by 1, 2, 3, 4 having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0;";

pub const DELETE_DIFFS: &str = "delete from bal.tokens_diffs where tx_id = $1;";

pub struct TokenDiffQuery<'a> {
    pub tx_id: &'a str,
}

impl TokenDiffQuery<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_DIFFS),
            args: vec![SQLArg::Text(String::from(self.tx_id))],
        }
    }
}

pub fn rollback_statement(tx_id: &str) -> SQLStatement {
    SQLStatement {
        sql: String::from(DELETE_DIFFS),
        args: vec![SQLArg::Text(String::from(tx_id))],
    }
}

pub mod bootstrapping {
    use crate::db::SQLArg;
    use crate::db::SQLStatement;

    pub const INSERT_DIFFS_AT_HEIGHT: &str = "
    with inputs as (
        with transactions as (	
            select height, id
            from core.transactions
            where height = $1
        )
        select tx.height
            , tx.id as tx_id
            , op.address
            , ba.token_id
            , sum(ba.amount) as value
        from transactions tx
        join core.inputs ip on ip.tx_id = tx.id
        join core.outputs op on op.box_id = ip.box_id
        join core.box_assets ba on ba.box_id = ip.box_id
        group by 1, 2, 3, 4
    ), outputs as (
        with transactions as (	
            select height, id
            from core.transactions
            where height = $1
        )
        select tx.height
            , tx.id as tx_id
            , op.address
            , ba.token_id
            , sum(ba.amount) as value
        from transactions tx
        join core.outputs op on op.tx_id = tx.id
        join core.box_assets ba on ba.box_id = op.box_id
        group by 1, 2, 3, 4
    )
    insert into bal.tokens_diffs (address, token_id, height, tx_id, value)
    select coalesce(i.address, o.address) as address
        , coalesce(i.token_id, o.token_id ) as token_id
        , coalesce(i.height, o.height) as height
        , coalesce(i.tx_id, o.tx_id) as tx_id
        , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) as value
    from inputs i
    full outer join outputs o
        on o.address = i.address
        and o.tx_id = i.tx_id
        and o.token_id = i.token_id
    group by 1, 2, 3, 4 having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0;";

    pub fn insert_diffs_statement(height: i32) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_DIFFS_AT_HEIGHT),
            args: vec![SQLArg::Integer(height)],
        }
    }
}

pub mod constraints {
    pub const ADD_PK: &str =
        "alter table bal.tokens_diffs add primary key(address, token_id, height, tx_id);";
    pub const IDX_HEIGHT: &str = "create index on bal.tokens_diffs(height);";
}

use crate::db::SQLArg;
use crate::db::SQLStatement;

// TODO include tx details (for age calcs, not so much for balance)

pub const INSERT_DIFF: &str = "\
    with inputs as (
        select tx.height
            , tx.id as tx_id
            , op.address
            , sum(op.value) as value
        from core.transactions tx
        join core.inputs ip on ip.tx_id = tx.id
        join core.outputs op on op.box_id = ip.box_id
        where tx.id = $1
        group by 1, 2, 3
    ), outputs as (
        select tx.height
            , tx.id as tx_id
            , op.address
            , sum(op.value) as value
        from core.transactions tx
        join core.outputs op on op.tx_id = tx.id
        where tx.id = $1
        group by 1, 2, 3
    )
    insert into bal.erg_diffs (address, height, tx_id, value)
    select coalesce(i.address, o.address) as address
        , coalesce(i.height, o.height) as height
        , coalesce(i.tx_id, o.tx_id) as tx_id
        , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0))
    from inputs i
    full outer join outputs o on o.address = i.address
    group by 1, 2, 3 having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0";

// pub const BOOTSTRAP_DIFF: &str = "
//     with transactions as (
//         select id
//             , height
//         from core.transactions
//     ), inputs as (
//         select tx.height
//             , tx.id as tx_id
//             , op.address
//             , sum(op.value) as value
//         from transactions tx
//         join core.inputs ip on ip.tx_id = tx.id
//         join core.outputs op on op.box_id = ip.box_id
//         group by 1, 2, 3
//     ), outputs as (
//         select tx.height
//             , tx.id as tx_id
//             , op.address
//             , sum(op.value) as value
//         from transactions tx
//         join core.outputs op on op.tx_id = tx.id
//         group by 1, 2, 3
//     )
//     insert into bal.erg_diffs (address, height, tx_id, value)
//     select left(coalesce(i.address, o.address), 20) as address
//         , coalesce(i.height, o.height) as height
//         , coalesce(i.tx_id, o.tx_id) as tx_id
//         , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0))
//     from inputs i
//     full outer join outputs o on o.address = i.address and o.tx_id = i.tx_id
//     group by 1, 2, 3
//     having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0
//     ;";

pub struct ErgDiffQuery<'a> {
    pub tx_id: &'a str,
}

impl ErgDiffQuery<'_> {
    pub fn to_statement(&self) -> SQLStatement {
        SQLStatement {
            sql: String::from(INSERT_DIFF),
            args: vec![SQLArg::Text(String::from(self.tx_id))],
        }
    }
}

Reuse diffs to update balances...

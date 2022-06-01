use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::Transaction;

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let statement = tx
        .prepare_typed(INSERT_DIFFS_FOR_TX, &[Type::TEXT])
        .unwrap();

    let tx_ids: Vec<&str> = block.transactions.iter().map(|tx| tx.id).collect();

    for tx_id in tx_ids {
        tx.execute(&statement, &[&tx_id]).unwrap();
    }
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    let statement = tx
        .prepare_typed(DELETE_DIFFS_FOR_TX, &[Type::TEXT])
        .unwrap();
    let tx_ids: Vec<&str> = block.transactions.iter().map(|tx| tx.id).collect();
    for tx_id in tx_ids {
        tx.execute(&statement, &[&tx_id]).unwrap();
    }
}

const INSERT_DIFFS_FOR_TX: &str = "
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

const DELETE_DIFFS_FOR_TX: &str = "delete from bal.tokens_diffs where tx_id = $1;";

pub(super) const INSERT_DIFFS_FOR_HEIGHT: &str = "
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

pub fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table bal.tokens_diffs add primary key(address, token_id, height, tx_id);",
        "alter table bal.tokens_diffs alter column address set not null;",
        "alter table bal.tokens_diffs alter column token_id set not null;",
        "alter table bal.tokens_diffs alter column height set not null;",
        "alter table bal.tokens_diffs alter column tx_id set not null;",
        "alter table bal.tokens_diffs alter column value set not null;",
        "create index on bal.tokens_diffs(height);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

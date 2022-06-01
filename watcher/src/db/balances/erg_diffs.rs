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

const DELETE_DIFFS_FOR_TX: &str = "delete from bal.erg_diffs where tx_id = $1;";

pub const INSERT_DIFFS_FOR_HEIGHT: &str = "
    with transactions as (	
        select height, id
        from core.transactions
        where height = $1
    ), inputs as (
        select tx.height
            , tx.id as tx_id
            , op.address
            , sum(op.value) as value
        from transactions tx
        join core.inputs ip on ip.tx_id = tx.id
        join core.outputs op on op.box_id = ip.box_id
        group by 1, 2, 3
    ), outputs as (
        select tx.height
            , tx.id as tx_id
            , op.address
            , sum(op.value) as value
        from transactions tx
        join core.outputs op on op.tx_id = tx.id
        group by 1, 2, 3
    )
    insert into bal.erg_diffs (address, height, tx_id, value)
    select coalesce(i.address, o.address) as address
        , coalesce(i.height, o.height) as height
        , coalesce(i.tx_id, o.tx_id) as tx_id
        , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) as value
    from inputs i
    full outer join outputs o
        on o.address = i.address
        and o.tx_id = i.tx_id
    group by 1, 2, 3 having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0;";

pub fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table bal.erg_diffs add primary key(address, height, tx_id);",
        "alter table bal.erg_diffs alter column address set not null;",
        "alter table bal.erg_diffs alter column height set not null;",
        "alter table bal.erg_diffs alter column tx_id set not null;",
        "alter table bal.erg_diffs alter column value set not null;",
        "create index on bal.erg_diffs(height);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

use crate::parsing::BlockData;
use postgres::Transaction;

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    tx.execute(INSERT_DIFFS_FOR_HEIGHT, &[&block.height])
        .unwrap();
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(
        "delete from bal.erg_diffs where height = $1;",
        &[&block.height],
    )
    .unwrap();
}

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

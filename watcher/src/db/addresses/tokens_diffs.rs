use crate::parsing::BlockData;
use postgres::Transaction;

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    tx.execute(INSERT_DIFFS_FOR_HEIGHT, &[&block.height])
        .unwrap();
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(
        "delete from adr.tokens_diffs where height = $1;",
        &[&block.height],
    )
    .unwrap();
}

pub(super) const INSERT_DIFFS_FOR_HEIGHT: &str = "
with inputs as (
    with transactions as (	
        select height, id
        from core.transactions
        where height = $1
    )
    select tx.height
        , tx.id as tx_id
        , op.address_id
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
        , op.address_id
        , ba.token_id
        , sum(ba.amount) as value
    from transactions tx
    join core.outputs op on op.tx_id = tx.id
    join core.box_assets ba on ba.box_id = op.box_id
    group by 1, 2, 3, 4
)
insert into adr.tokens_diffs (address_id, token_id, height, tx_id, value)
select coalesce(i.address_id, o.address_id) as address_id
    , coalesce(i.token_id, o.token_id ) as token_id
    , coalesce(i.height, o.height) as height
    , coalesce(i.tx_id, o.tx_id) as tx_id
    , sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) as value
from inputs i
full outer join outputs o
    on o.address_id = i.address_id
    and o.tx_id = i.tx_id
    and o.token_id = i.token_id
group by 1, 2, 3, 4 having sum(coalesce(o.value, 0)) - sum(coalesce(i.value, 0)) <> 0;";

pub fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table adr.tokens_diffs add primary key(address_id, token_id, height, tx_id);",
        "alter table adr.tokens_diffs alter column address_id set not null;",
        "alter table adr.tokens_diffs alter column token_id set not null;",
        "alter table adr.tokens_diffs alter column height set not null;",
        "alter table adr.tokens_diffs alter column tx_id set not null;",
        "alter table adr.tokens_diffs alter column value set not null;",
        "create index on adr.tokens_diffs(height);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

use crate::parsing::BlockData;
use postgres::Transaction;

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let height = &block.height;
    tx.execute(UPDATE_BALANCES, &[height]).unwrap();
    tx.execute(INSERT_BALANCES, &[height]).unwrap();
    tx.execute(DELETE_ZERO_BALANCES, &[]).unwrap();
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    let height = &block.height;
    tx.execute(ROLLBACK_DELETE_ZERO_BALANCES, &[height])
        .unwrap();
    tx.execute(ROLLBACK_BALANCE_UPDATES, &[height]).unwrap();
    tx.execute(DELETE_ZERO_BALANCES, &[]).unwrap();
}

// Updates balances for known addresses
pub(super) const UPDATE_BALANCES: &str = "
    with new_diffs as (
        select address_id
            , token_id
            , sum(value) as value
        from adr.tokens_diffs
        where height = $1
        group by 1, 2
    )
    update adr.tokens b
    set value = b.value + d.value
    from new_diffs d
    where d.address_id = b.address_id
        and d.token_id = b.token_id;";

// Inserts balances for new addresses
pub(super) const INSERT_BALANCES: &str = "
    with new_addresses as (
        select d.address_id
            , d.token_id
            , sum(d.value) as value
        from adr.tokens_diffs d
        left join adr.tokens b
            on b.address_id = d.address_id
            and b.token_id = d.token_id
        where d.height = $1
            and b.address_id is null
        group by 1, 2
    )
    insert into adr.tokens(address_id, token_id, value)
    select address_id
        , token_id
        , value
    from new_addresses;";

pub(super) const DELETE_ZERO_BALANCES: &str = "
    delete from adr.tokens
    where value = 0;";

// Undo balance updates
const ROLLBACK_BALANCE_UPDATES: &str = "
    with new_diffs as (
        select address_id
            , token_id
            , sum(value) as value
        from adr.tokens_diffs
        where height = $1
        group by 1, 2
    )
    update adr.tokens b
    set value = b.value - d.value
    from new_diffs d
    where d.address_id = b.address_id
        and d.token_id = b.token_id;";

const ROLLBACK_DELETE_ZERO_BALANCES: &str = "
    with deleted_addresses as (
        select d.address_id
            , d.token_id
            , sum(d.value) as value
        from adr.tokens_diffs d
        left join adr.tokens b
            on b.address_id = d.address_id
            and b.token_id = d.token_id
        where d.height = $1
            and b.address_id is null
        group by 1, 2
    )
    insert into adr.tokens(address_id, token_id, value)
    select address_id
        , token_id
        , 0 -- actual value will be set by update rollback
    from deleted_addresses;";

pub fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table adr.tokens add primary key(address_id, token_id);",
        "alter table adr.tokens alter column address_id set not null;",
        "alter table adr.tokens alter column token_id set not null;",
        "alter table adr.tokens alter column value set not null;",
        "alter table adr.tokens add check (value >= 0);",
        "create index on adr.tokens(value);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}
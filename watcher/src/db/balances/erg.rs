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

// Inserts balances for new addresses
pub(super) const INSERT_BALANCES: &str = "
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

pub(super) const DELETE_ZERO_BALANCES: &str = "
    delete from bal.erg
    where value = 0;";

// Undo balance updates
const ROLLBACK_BALANCE_UPDATES: &str = "
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

const ROLLBACK_DELETE_ZERO_BALANCES: &str = "
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

pub fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table bal.erg add primary key(address);",
        "alter table bal.erg alter column address set not null;",
        "alter table bal.erg alter column value set not null;",
        "alter table bal.erg add check (value >= 0);",
        "create index on bal.erg(value);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

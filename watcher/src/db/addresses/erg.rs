use crate::parsing::BlockData;
use postgres::Transaction;

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let height = &block.height;
    let ts = &block.timestamp;
    tx.execute(UPDATE_BALANCES, &[height, &ts]).unwrap();
    tx.execute(INSERT_BALANCES, &[height, &ts]).unwrap();
    tx.execute(DELETE_ZERO_BALANCES, &[]).unwrap();
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    let height = &block.height;
    // Rollback any addresses that were not deleted (i.e. balance did not drop to zero)
    tx.execute(ROLLBACK_BALANCE_UPDATES, &[height, &block.timestamp])
        .unwrap();
    // Then, restore deleted addresses from scratch
    tx.execute(ROLLBACK_DELETE_ZERO_BALANCES, &[height])
        .unwrap();
    tx.execute(DELETE_ZERO_BALANCES, &[]).unwrap();
}

// Updates balances for known addresses
pub(super) const UPDATE_BALANCES: &str = "
    with new_diffs as (
        select address_id
            , sum(value) as value
        from adr.erg_diffs
        where height = $1
        group by 1
    )
    update adr.erg a
    set value = a.value + d.value
        , mean_age_timestamp = case 
            when d.value > 0 then
                a.value / (a.value + d.value) * a.mean_age_timestamp + d.value / (a.value + d.value) * $2::bigint
            when d.value = -a.value then 0
            else a.mean_age_timestamp
        end
    from new_diffs d
    where d.address_id = a.address_id;";

// Inserts balances for new addresses
pub(super) const INSERT_BALANCES: &str = "
    with new_addresses as (
        select d.address_id
            , sum(d.value) as value
        from adr.erg_diffs d
        left join adr.erg b on b.address_id = d.address_id
        where d.height = $1
            and b.address_id is null
        group by 1
    )
    insert into adr.erg(address_id, value, mean_age_timestamp)
    select address_id
        , value
        , $2
    from new_addresses;";

pub(super) const DELETE_ZERO_BALANCES: &str = "
    delete from adr.erg
    where value = 0;";

// Undo balance updates for addresses still having some balance
const ROLLBACK_BALANCE_UPDATES: &str = "
    with updated_addresses_diffs as (
        select d.address_id
            , sum(d.value) as value
        from adr.erg_diffs d
        join adr.erg a on a.address_id = d.address_id
        where d.height = $1
        group by 1
    )
    update adr.erg a
    set value = a.value - d.value
        , mean_age_timestamp = case
            when (a.value - d.value) <> 0 then
                a.mean_age_timestamp / (a.value - d.value) * (a.value + d.value) - d.value / (a.value - d.value) * $2::bigint
            else 0
        end
    from updated_addresses_diffs d
    where d.address_id = a.address_id;";

// Restore deleted addresses from scratch.
//
// Precalc deleted balances to avoid bigint overflows
const ROLLBACK_DELETE_ZERO_BALANCES: &str = "
    with deleted_addresses as (
        select distinct d.address_id
        from adr.erg_diffs d
        left join adr.erg a on a.address_id = d.address_id
        where d.height = $1
            and a.address_id is null
    ), deleted_balances as (
        select d.address_id
            , sum(d.value) as prev_balance
        from deleted_addresses x
        join adr.erg_diffs d on d.address_id = x.address_id
        where d.height < $1
        group by 1 having sum(d.value) <> 0
    )
    -- recalc from scratch
    insert into adr.erg(address_id, value, mean_age_timestamp)
        select d.address_id
            , x.prev_balance
            , sum(d.value / x.prev_balance * h.timestamp)
        from deleted_balances x
        join adr.erg_diffs d on d.address_id = x.address_id
        join core.headers h on h.height = d.height
        where d.height < $1
        group by 1, 2
        having sum(d.value) <> 0;";

pub fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table adr.erg add primary key(address_id);",
        "alter table adr.erg alter column address_id set not null;",
        "alter table adr.erg alter column value set not null;",
        "alter table adr.erg alter column mean_age_timestamp set not null;",
        "alter table adr.erg add check (value >= 0);",
        "create index on adr.erg(value);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

pub mod replay {
    use postgres::Transaction;

    /// Create an instance of the adr.erg table as it was at `height`.
    ///
    /// New table is created as {id}_adr.erg.
    pub fn prepare(tx: &mut Transaction, height: i32, id: &str) {
        tx.execute(
            &format!(
                "
                create table {id}_adr.erg as
                    with balances as (
                        select address_id
                            , sum(value) as value
                        from adr.erg_diffs
                        where height <= $1
                        group by 1 having sum(value) > 0
                    )
                    select d.address_id
                        , b.value
                        , sum(d.value / b.value * h.timestamp) as mean_age_timestamp
                    from adr.erg_diffs d
                    join balances b on b.address_id = d.address_id
                    join core.headers h on h.height = d.height
                    where h.height <= $1
                    group by 1, 2;"
            ),
            &[&height],
        )
        .unwrap();

        // Same constraints as synced table
        tx.execute(
            &format!("alter table {id}_adr.erg add primary key (address_id);"),
            &[],
        )
        .unwrap();
        tx.execute(
            &format!("alter table {id}_adr.erg alter column address_id set not null;"),
            &[],
        )
        .unwrap();
        tx.execute(
            &format!("alter table {id}_adr.erg alter column value set not null;"),
            &[],
        )
        .unwrap();
        tx.execute(
            &format!("alter table {id}_adr.erg alter column mean_age_timestamp set not null;"),
            &[],
        )
        .unwrap();
        tx.execute(
            &format!("alter table {id}_adr.erg add check (value >= 0);"),
            &[],
        )
        .unwrap();
        tx.execute(&format!("create index on {id}_adr.erg(value);"), &[])
            .unwrap();
    }

    /// Advance state of replay table {id}_adr.erg) to next `height`.
    ///
    /// Assumes current state of replay table is at `height` - 1.
    pub fn step(tx: &mut Transaction, height: i32, id: &str) {
        // Update known addresses
        tx.execute(&format!(
            "
            with new_diffs as (
                select address_id
                    , sum(value) as value
                from adr.erg_diffs
                where height = $1
                group by 1
            ), timestamp as (
                select timestamp
                from core.headers
                where height = $1
            )
            update {id}_adr.erg a
            set value = a.value + d.value
                , mean_age_timestamp = case 
                    when a.value + d.value <> 0 then
                        a.value * a.mean_age_timestamp / (a.value + d.value) + d.value * t.timestamp / (a.value + d.value)
                    else 0
                end
            from new_diffs d, timestamp t
            where d.address_id = a.address_id;"),
            &[&height]
        ).unwrap();

        // Insert new addresses
        tx.execute(
            &format!(
                "
                with new_addresses as (
                    select d.address_id
                        , sum(d.value) as value
                    from adr.erg_diffs d
                    left join {id}_adr.erg b on b.address_id = d.address_id
                    where d.height = $1
                        and b.address_id is null
                    group by 1
                )
                insert into {id}_adr.erg(address_id, value, mean_age_timestamp)
                select address_id
                    , value
                    , (select timestamp from core.headers where height = $1)
                from new_addresses;"
            ),
            &[&height],
        )
        .unwrap();

        // Delete zero balances
        tx.execute(&format!("delete from {id}_adr.erg where value = 0;"), &[])
            .unwrap();
    }
}

use crate::db::metrics::supply_age::SupplyAgeDiffs;
use crate::parsing::BlockData;
use postgres::Transaction;

pub(super) fn include(tx: &mut Transaction, block: &BlockData) -> SupplyAgeDiffs {
    let height = &block.height;
    let ts = &block.timestamp;
    tx.execute(UPDATE_BALANCES, &[height, &ts]).unwrap();
    tx.execute(INSERT_BALANCES, &[height, &ts]).unwrap();

    let sad = SupplyAgeDiffs::get(tx, block.height, block.timestamp);
    tx.execute(DELETE_ZERO_BALANCES, &[]).unwrap();
    sad
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    let height = &block.height;
    // Rollback any addresses that were not deleted (i.e. balance did not drop to zero)
    tx.execute(ROLLBACK_BALANCE_UPDATES, &[height, &block.timestamp])
        .unwrap();
    // Then, restore deleted addresses from scratch
    let addresses = get_spent_addresses(tx, block.height);
    for address_id in addresses {
        restore_spent_address(tx, address_id, block.height);
    }

    // Finally
    tx.execute(DELETE_ZERO_BALANCES, &[]).unwrap();
}

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

/// Get addresses that got spent at given `height`.
fn get_spent_addresses(tx: &mut Transaction, height: i32) -> Vec<i64> {
    tx.query(
        "
        select d.address_id
        from adr.erg_diffs d
        left join adr.erg b on b.address_id = d.address_id
        where d.height = $1
            -- Limit to spent addresses (i.e. no more balance)
            and b.address_id is null
        -- Limit to addresses that had an existing balance
        group by 1 having sum(d.value) < 0;",
        &[&height],
    )
    .unwrap()
    .iter()
    .map(|r| r.get(0))
    .collect()
}

/// Roll back a spent address.
///
/// Calculates address balance and age as they were prior to given `height`
/// and inserts it back into adr.erg.
fn restore_spent_address(tx: &mut Transaction, address_id: i64, height: i32) {
    tx.execute("
        with recursive diffs as (
            select row_number() over (order by h.height, t.index) as rn
                , h.timestamp as ts
                , d.value as diff
            from adr.erg_diffs d
            join core.transactions t on t.id = d.tx_id
            join core.headers h on h.height = d.height
            where d.address_id = $1
                -- ignore current block!
                and d.height < $2
        )
        , rec_query(rn, ts, diff, bal, mat) as (
            select rn, ts, diff, diff, ts from diffs where rn = 1

            union all

            select n.rn
                , n.ts
                , n.diff
                , p.bal + n.diff
                , case
                    when n.diff = p.bal then 0
                    when n.diff < 0 then p.mat
                    else p.bal::numeric * p.mat / (p.bal + n.diff) + n.diff::numeric * n.ts / (p.bal + n.diff)
                end::bigint
            from rec_query p
            join diffs n on n.rn = p.rn + 1
        )
        insert into adr.erg(address_id, value, mean_age_timestamp)
        select $1
            , bal
            , mat
        from rec_query
        order by rn desc limit 1;", &[&address_id, &height]).unwrap();
}

pub mod replay {
    use crate::db::metrics::supply_age::SupplyAgeDiffs as SAD;
    use postgres::Transaction;

    /// Create an instance of the adr.erg table as it was at `height`.
    ///
    /// New table is created as {id}_adr.erg.
    ///
    /// Mean age timestamps cannot be calculated (cheaply), so set to zero.
    /// Replay for age should always start at -1 and step from there.
    pub fn prepare(tx: &mut Transaction, height: i32, id: &str) {
        tx.execute(
            &format!(
                "
                create table {id}_adr.erg as
                    select address_id
                        , sum(value) as value
                    from adr.erg_diffs
                    where height <= $1
                    group by 1 having sum(value) > 0;"
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
            &format!("alter table {id}_adr.erg add check (value >= 0);"),
            &[],
        )
        .unwrap();
        tx.execute(&format!("create index on {id}_adr.erg(value);"), &[])
            .unwrap();
    }

    /// Create empty instance of the adr.erg table.
    ///
    /// New table is created as {id}_adr.erg.
    pub fn create_with_age(tx: &mut Transaction, id: &str) {
        tx.execute(
            &format!(
                "
                create table {id}_adr.erg (
                    address_id bigint not null primary key,
                    value bigint not null,
                    mean_age_timestamp bigint not null,
                    check (value >= 0)
                );"
            ),
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
        tx.execute(
            &format!(
                "
                with new_diffs as (
                    select address_id
                        , sum(value) as value
                    from adr.erg_diffs
                    where height = $1
                    group by 1
                )
                update {id}_adr.erg a
                set value = a.value + d.value
                from new_diffs d
                where d.address_id = a.address_id;"
            ),
            &[&height],
        )
        .unwrap();

        // Insert new addresses
        tx.execute(
            &format!(
                "
                insert into {id}_adr.erg(address_id, value)
                    select d.address_id
                        , sum(d.value) as value
                    from adr.erg_diffs d
                    left join {id}_adr.erg b on b.address_id = d.address_id
                    where d.height = $1
                        and b.address_id is null
                    group by 1 having sum(d.value) <> 0;"
            ),
            &[&height],
        )
        .unwrap();

        // Delete zero balances
        tx.execute(&format!("delete from {id}_adr.erg where value = 0;"), &[])
            .unwrap();
    }

    /// Advance state of replay table {id}_adr.erg) to next `height`.
    ///
    /// Assumes current state of replay table is at `height` - 1.
    pub fn step_with_age(tx: &mut Transaction, height: i32, id: &str) -> SAD {
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
                    when d.value > 0 then
                        a.value / (a.value + d.value) * a.mean_age_timestamp + d.value / (a.value + d.value) * t.timestamp
                    else a.mean_age_timestamp
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

        // Get timestamp of block h
        let ts: i64 = tx
            .query_one(
                "
            select timestamp from core.headers where height = $1;",
                &[&height],
            )
            .unwrap()
            .get(0);

        assert_eq!(id, "mtr_sa");
        let supply_age_diffs = SAD::get_mtr_sa(tx, height, ts);

        // Delete zero balances
        tx.execute(&format!("delete from {id}_adr.erg where value = 0;"), &[])
            .unwrap();

        supply_age_diffs
    }
}

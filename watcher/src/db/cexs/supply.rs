use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::GenericClient;
use postgres::Transaction;
use std::collections::HashMap;

/// Main and deposit supply differences, in nano ERG.
struct SupplyDiff {
    cex_id: i32,
    main: i64,
    deposit: i64,
}

/// Record cex supply changes
pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    let supply_diffs = get_supply_diffs(tx, block.height);
    let statement = tx
        .prepare_typed(
            "
            insert into cex.supply (height, cex_id, main, deposit)
            values ($1, $2, $3, $4);",
            &[Type::INT4, Type::INT4, Type::INT8, Type::INT8],
        )
        .unwrap();

    for sd in supply_diffs {
        // Update cache
        *cache.main.entry(sd.cex_id).or_insert(0) += sd.main;
        *cache.deposit.entry(sd.cex_id).or_insert(0) += sd.deposit;

        // Update db
        tx.execute(
            &statement,
            &[
                &block.height,
                &sd.cex_id,
                &cache.main[&sd.cex_id],
                &cache.deposit[&sd.cex_id],
            ],
        )
        .unwrap();
    }
}

/// Remove deposit addresses
pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    tx.execute(
        "delete from cex.supply where height = $1;",
        &[&block.height],
    )
    .unwrap();

    // Update cache
    let supply_diffs = get_supply_diffs(tx, block.height);
    for sd in supply_diffs {
        *cache.main.get_mut(&sd.cex_id).unwrap() -= sd.main;
        *cache.deposit.get_mut(&sd.cex_id).unwrap() -= sd.deposit;
    }
}

/// Non-zero supply diffs, by cex, relative to supply at previous height.
fn get_supply_diffs(tx: &mut Transaction, height: i32) -> Vec<SupplyDiff> {
    let rows = tx
        .query(
            "
            with main_addresses_diffs as (
                select a.cex_id
                    , sum(d.value)::bigint as value
                from cex.main_addresses a
                join adr.erg_diffs d
                    on d.address_id = a.address_id
                where d.height = $1
                group by 1
            ), deposit_addresses_diffs as (
                select a.cex_id
                    , sum(d.value)::bigint as value
                from cex.deposit_addresses a
                join adr.erg_diffs d
                    on d.address_id = a.address_id
                where d.height = $1
                group by 1
            )
            select coalesce(m.cex_id, d.cex_id) as cex_id
                , coalesce(m.value, 0)::bigint as main
                , coalesce(d.value, 0)::bigint as deposit
            from main_addresses_diffs m
            full outer join deposit_addresses_diffs d
                on d.cex_id = m.cex_id
            ",
            &[&height],
        )
        .unwrap();

    rows.iter()
        .map(|r| SupplyDiff {
            cex_id: r.get(0),
            main: r.get(1),
            deposit: r.get(2),
        })
        .filter(|sd| sd.main != 0 || sd.deposit != 0)
        .collect()
}

#[derive(Debug)]
pub struct Cache {
    /// Maps cex_id to latest supply on its main addresses
    pub main: HashMap<i32, i64>,
    /// Maps cex_id to latest supply on its deposit addresses
    pub deposit: HashMap<i32, i64>,
}

impl Cache {
    pub(super) fn new() -> Self {
        Self {
            main: HashMap::new(),
            deposit: HashMap::new(),
        }
    }

    pub(super) fn load(client: &mut impl GenericClient) -> Self {
        // Main and deposit supplies
        let rows = client
            .query(
                "
                select cex_id
                    , main
                    , deposit
                from cex.supply
                where (cex_id, height) in (
                    select cex_id
                        , max(height)
                    from cex.supply
                    group by 1
                );",
                &[],
            )
            .unwrap();
        let mut c = Cache::new();
        for row in rows {
            let cex_id: i32 = row.get(0);
            c.main.insert(cex_id, row.get(1));
            c.deposit.insert(cex_id, row.get(2));
        }
        c
    }
}

pub(super) fn process_deposit_addresses(
    tx: &mut Transaction,
    queues: &super::deposit_addresses::AddressQueues,
    cache: &mut Cache,
) {
    if !queues.propagate.is_empty() {
        propagate_deposit_addresses(tx, &queues.propagate);
    }
    if !queues.purge.is_empty() {
        purge_deposit_addresses(tx);
    }
    *cache = Cache::load(tx);
}

/// Reflect addition of new deposit addresses
fn propagate_deposit_addresses(tx: &mut Transaction, addresses: &Vec<i64>) {
    // Prepare patch: deposit balance changes by height and cex_id
    tx.execute(
        "
        create temp table _cex_supply_patch as
            with diffs as (
                select d.height
                    , a.cex_id
                    , sum(d.value) as value
                from adr.erg_diffs d
                join cex.deposit_addresses a on a.address_id = d.address_id
                where d.address_id = any($1)
                group by 1, 2
            ), first_diffs as (
                select cex_id
                    , min(height) as height
                from diffs
                group by 1 
            -- Collect all existing records to be updated to ensure they are included
            ), existing_records as (
                select s.height
                    , s.cex_id
                    , 0 as value -- zero diff value, actual values will come from diffs cte
                from cex.supply s, first_diffs d
                where d.cex_id = s.cex_id
                    and s.height >= d.height
            ), full_patch_diffs as (
                select height
                    , cex_id
                    , sum(value) as value
                from (
                    select height, cex_id, value
                    from diffs
                    union
                    select height, cex_id, value
                    from existing_records
                ) sq
                group by 1, 2
            )
            select height
                , cex_id
                , sum(value) over (
                    partition by cex_id
                    order by height asc
                    rows between unbounded preceding and current row
                ) as value
            from full_patch_diffs
            order by 1;
        ",
        &[&addresses],
    )
    .unwrap();

    // Add index to speed things up
    tx.execute(
        "alter table _cex_supply_patch add primary key(height, cex_id);",
        &[],
    )
    .unwrap();

    // Insert new heights with balances of latest height prior
    tx.execute(
        "
        with new_entries as (
            select p.height
                , p.cex_id
            from _cex_supply_patch p
            left join cex.supply s
                on s.height = p.height
                and s.cex_id = p.cex_id
            where s.height is null
        )
        insert into cex.supply(height, cex_id, main, deposit)
            select p.new_height
                , p.cex_id
                , coalesce(s.main, 0)
                , coalesce(s.deposit, 0)
            from (
                select n.height as new_height
                    , n.cex_id
                    , max(s.height) as prev_height
                from new_entries n
                left join cex.supply s on s.cex_id = n.cex_id and s.height < n.height
                group by 1, 2
            ) p
            left join cex.supply s
                on s.height = p.prev_height
                and s.cex_id = p.cex_id;
        ",
        &[],
    )
    .unwrap();

    // Update deposit balances with patches
    tx.execute(
        "
        update cex.supply s
        set deposit = s.deposit + p.value
        from _cex_supply_patch p
        where (p.height, p.cex_id) = (s.height, s.cex_id);
        ",
        &[],
    )
    .unwrap();
}

fn purge_deposit_addresses(tx: &mut Transaction) {
    // Recalc form scratch
    tx.execute("truncate cex.supply", &[]).unwrap();
    tx.execute(
        "
        with main_diffs as (
            select d.height
                , c.cex_id
                , sum(d.value) as value
            from cex.main_addresses c
            join adr.erg_diffs d on d.address_id = c.address_id
            group by 1, 2
            having sum(d.value) <> 0
        ), deposit_diffs as (
            select d.height
                , c.cex_id
                , sum(d.value) as value
            from cex.deposit_addresses c
            join adr.erg_diffs d on d.address_id = c.address_id
            group by 1, 2
            having sum(d.value) <> 0
        ), merged as (
            select coalesce(m.height, d.height) as height
                , coalesce(m.cex_id, d.cex_id) as cex_id
                , coalesce(m.value, 0) as main
                , coalesce(d.value, 0) as deposit
            from main_diffs m
            full outer join deposit_diffs d
                on d.height = m.height
                and d.cex_id = m.cex_id
        )
        insert into cex.supply (height, cex_id, main, deposit)
            select height
                , cex_id
                , sum(main) over w as main
                , sum(deposit) over w as deposit
            from merged
            window w as (
                partition by cex_id
                order by height asc
                rows between unbounded preceding and current row
            )
            order by 1, 2;",
        &[],
    )
    .unwrap();
}

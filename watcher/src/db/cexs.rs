use crate::parsing::BlockData;
use log::debug;
use log::info;
use postgres::Client;
use std::collections::HashMap;

mod addresses;
mod processing_log;
mod supply;

use postgres::Transaction;

pub fn include_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    let invalidation_height: Option<i32> = addresses::include(tx, block);
    processing_log::include(tx, block, invalidation_height);
    supply::include(tx, block, cache);
    Ok(())
}

pub fn rollback_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    supply::rollback(tx, block, cache);
    addresses::rollback(tx, block);
    processing_log::rollback(tx, block);
    Ok(())
}

/// Find all deposit addresses from first to last available block.
pub fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    if is_bootstrapped(tx) {
        return Ok(());
    }
    info!("Bootstrapping CEX data (addresses)");

    // Create work table
    tx.execute(
        "
        create unlogged table cex._bootstrapping_data (
            address text,
            cex_id int,
            spot_height int, 
            first_tx_height int,
            primary key(address, cex_id)
        );",
        &[],
    )?;

    // Add indexes to speed up join in bootstrapping query
    tx.execute("create index on cex._bootstrapping_data (cex_id);", &[])?;
    tx.execute("create index on cex._bootstrapping_data (address);", &[])?;

    // Procedure to collect new deposit addresses, their spot height
    // and first tx height.
    tx.execute(
        "
        create procedure cex._find_new_deposit_addresses(_height int) as $$
            with to_main_txs as ( 
                select cas.cex_id
                    , dif.tx_id
                    , dif.value
                    , cas.address as main_address
                from cex.addresses cas
                join bal.erg_diffs dif on dif.address = cas.address
                where cas.type = 'main'
                    and dif.height = _height
                    and dif.value > 0
            ), deposit_addresses as (
                select dif.address
                    , txs.cex_id 
                from bal.erg_diffs dif
                join to_main_txs txs on txs.tx_id = dif.tx_id
                -- be aware of known addresses
                left join cex.addresses cas
                    on cas.address = dif.address
                    and cas.cex_id = txs.cex_id
                left join cex._bootstrapping_data bsd
                    on bsd.address = dif.address
                    and bsd.cex_id = txs.cex_id
                where dif.value < 0
                    and dif.height = _height
                    -- exclude txs from known addresses
                    and cas.address is null
                    and bsd.address is null
                    -- exclude contract addresses
                    and starts_with(dif.address, '9')
                    and length(dif.address) = 51
                -- dissolve duplicates from multiple txs in same block
                group by 1, 2
            )
            insert into cex._bootstrapping_data (
                address,
                cex_id,
                spot_height,
                first_tx_height
            )
            select das.address
                , das.cex_id
                , _height
                , min(dif.height)
            from deposit_addresses das
            join bal.erg_diffs dif on dif.address = das.address
            where dif.height <= _height
            group by 1, 2;
        $$
        language sql;",
        &[],
    )?;

    // Call above procedures for every known block
    tx.execute(
        "
        do language plpgsql $$
        declare
            _h int;
        begin
            for _h in
                select height
                from core.headers
                order by 1
            loop
                call cex._find_new_deposit_addresses(_h);
            end loop;
        end;
        $$;",
        &[],
    )?;

    // Find conflicting addresses in bootstrapping data
    tx.execute(
        "
        with conflicts as (
            select address
                , array_agg(cex_id order by spot_height) as cex_ids
                , array_agg(spot_height order by spot_height) as spot_heights
            from cex._bootstrapping_data
            group by 1 having count(*) > 1
        )
        insert into cex.addresses_conflicts (
            address,
            first_cex_id,
            type,
            spot_height,
            conflict_spot_height
        )
        select con.address
            , con.cex_ids[1]
            , coalesce(mas.type, 'deposit')
            , con.spot_heights[1]
            , con.spot_heights[2]
        from conflicts con
        left join cex.addresses mas
            on mas.address = con.address and mas.type = 'main'
        order by 4;",
        &[],
    )?;

    // Remove conflicting addresses from bootstrapping data
    tx.execute(
        "
        delete from cex._bootstrapping_data bsd
        using cex.addresses_conflicts con
        where con.address = bsd.address;",
        &[],
    )?;

    // Copy remaining deposit addresses
    tx.execute(
        "
        insert into cex.addresses (address, cex_id, type, spot_height)
        select address
            , cex_id
            , 'deposit'
            , spot_height
        from cex._bootstrapping_data
        order by spot_height;",
        &[],
    )?;

    // Log bootstrapped blocks as processed since dependent relations
    // are guaranteed to be generated later (i.e. dependents such as metrics
    // are bootstrapped after this).
    tx.execute(
        "
        insert into cex.block_processing_log (
            header_id,
            height,
            invalidation_height,
            status
        )
        select hds.id
            , hds.height
            , min(bsd.first_tx_height)
            , 'processed'
        from core.headers hds
        left join cex._bootstrapping_data bsd
            on bsd.spot_height = hds.height
        group by 1, 2
        order by 1;",
        &[],
    )?;

    // Cleanup
    tx.execute("drop procedure cex._find_new_deposit_addresses;", &[])?;
    tx.execute("drop table cex._bootstrapping_data;", &[])?;

    // Set constraint here so that the supply query can use the indexes.
    // TODO: consider setting supply constraints later.
    set_constraints(tx);

    info!("Bootstrapping CEX data (supply)");

    // Supply
    tx.execute(
        "
        with cex_diffs as (
            select d.height
                , c.cex_id
                , coalesce(sum(d.value) filter (where c.type = 'main'), 0) as main
                , coalesce(sum(d.value) filter (where c.type = 'deposit'), 0) as deposit
            from cex.addresses c
            join bal.erg_diffs d on d.address = c.address
            where height <= 500 * 1000
            group by 1, 2 having (
                sum(d.value) filter (where c.type = 'main') <> 0
                or
                sum(d.value) filter (where c.type = 'deposit') <> 0
            )
        )
        insert into cex.supply (height, cex_id, main, deposit)
            select height
                , cex_id
                , sum(main) over w as main
                , sum(deposit) over w as deposit
            from cex_diffs
            window w as (
                partition by cex_id
                order by height asc
                rows between unbounded preceding and current row
            )
            order by 1, 2;",
        &[],
    )?;
    Ok(())
}

fn is_bootstrapped(tx: &mut Transaction) -> bool {
    let row = tx
        .query_one(
            "
            select exists (select * from cex.block_processing_log limit 1);",
            &[],
        )
        .unwrap();
    row.get(0)
}

fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        // cexs
        "alter table cex.cexs add primary key (id);",
        "alter table cex.cexs add constraint cexs_unique_name unique (name);",
        // cexs addresses
        "alter table cex.addresses add primary key (address);",
        "alter table cex.addresses add foreign key (cex_id)
            references cex.cexs (id);",
        "alter table cex.addresses alter column type set not null;",
        "create index on cex.addresses(cex_id);",
        "create index on cex.addresses(type);",
        "create index on cex.addresses(spot_height);",
        // cexs addresses conflicts
        "alter table cex.addresses_conflicts add primary key (address);",
        "alter table cex.addresses_conflicts add foreign key (first_cex_id)
            references cex.cexs (id);",
        // cex.block_processing_log
        "alter table cex.block_processing_log add primary key (header_id);",
        "create index on cex.block_processing_log (status);",
        // cex.supply
        "alter table cex.supply add primary key (height, cex_id);",
        "alter table cex.supply add foreign key (cex_id)
            references cex.cexs (id);",
        "create index on cex.supply (height);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

pub struct Cache {
    /// Maps cex_id to latest supply on its main addresses
    pub(super) main_supply: HashMap<i32, i64>,
    /// Maps cex_id to latest supply on its deposit addresses
    pub(super) deposit_supply: HashMap<i32, i64>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            main_supply: HashMap::new(),
            deposit_supply: HashMap::new(),
        }
    }

    pub fn load(client: &mut Client) -> Self {
        debug!("Loading cexs cache");
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
            c.main_supply.insert(cex_id, row.get(1));
            c.deposit_supply.insert(cex_id, row.get(2));
        }
        c
    }

    pub fn load_at(client: &mut Client, height: i32) -> Self {
        debug!("Loading cexs cache for height {}", height);
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
                    where height <= $1
                    group by 1
                );",
                &[&height],
            )
            .unwrap();
        let mut c = Cache::new();
        for row in rows {
            let cex_id: i32 = row.get(0);
            c.main_supply.insert(cex_id, row.get(1));
            c.deposit_supply.insert(cex_id, row.get(2));
        }
        c
    }
}

pub(super) fn repair(tx: &mut Transaction, height: i32, cache: &mut Cache) {
    supply::repair(tx, height, cache);
}

pub mod repair {
    use super::processing_log;
    use postgres::Client;
    use postgres::Transaction;

    /// Get height at which repairs should start
    pub fn get_start_height(client: &mut Client, max_height: i32) -> Option<i32> {
        client
            .query_one(
                "
            select min(invalidation_height) as fr_height
            from cex.block_processing_log
            where height <= $1
                and (
                    status = 'pending'
                    or status = 'pending_rollback'
                );
            ",
                &[&max_height],
            )
            .unwrap()
            .get(0)
    }

    pub fn set_height_pending_to_processed(tx: &mut Transaction, height: i32) {
        processing_log::repair::set_height_pending_to_processed(tx, height);
    }

    pub fn process_non_invalidating_blocks(tx: &mut Transaction) {
        processing_log::repair::set_non_invalidating_blocks_to_processed(tx);
    }
}

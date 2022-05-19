use crate::parsing::BlockData;
use log::info;

mod addresses;
mod processing_log;

use postgres::Transaction;

/*
    New Main Address
    ================
    H is current height.
    1. Find first tx height of new main address (h)
    2. Loop over h..H
        - find new deposit addresses and keep them in a tmp table for (3)
    3. Derive height (h') of earliest deposit tx from new deposit addresses
    4. Move new deposit addresses from tmp table to cex.addresses.
    5. Loop over h'..H
        - update any dependents (e.g. metrics)
    All this would be called from within a migration.

    Normal block processing
    =======================
    H is height of block.
    1. Find new deposit addresses for H
    2. Save to cex.new_deposit_addresses

    Periodically:
    1. Derive height (h') of earliest deposit tx from new deposit addresses
    2. Update any dependents (e.g. metrics) from h' or flag an update is needed from h'

    The reason this is done periodically and not at each block is that
    deposit address may have very old receiving txs (e.g. when accumulating payouts).
    Boostrapping
    ============
    1. Find all deposit addresses to date.
    2. Bootstrap dependents (e.g. metrics)
*/

pub fn include_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<()> {
    let invalidation_height: Option<i32> = addresses::include(tx, block);
    processing_log::include(tx, block, invalidation_height);
    Ok(())
}

pub fn rollback_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<()> {
    addresses::rollback(tx, block);
    processing_log::rollback(tx, block);
    Ok(())
}

/// Find all deposit addresses from first to last available block.
pub fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    if is_bootstrapped(tx) {
        return Ok(());
    }
    info!("Bootstrapping CEX addresses");

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
                , array_agg(spot_height order by spot_height) as cex_ids
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
            , con.cex_ids[0]
            , coalesce(mas.type, 'deposit')
            , con.spot_heights[0]
            , con.spot_heights[1]
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

    set_constraints(tx);
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
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
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

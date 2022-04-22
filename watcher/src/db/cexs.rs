use crate::parsing::BlockData;
use log::info;

mod new_deposit_addresses;
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
    new_deposit_addresses::include(tx, block);
    processing_log::include(tx, block);
    Ok(())
}

pub fn rollback_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<()> {
    processing_log::rollback(tx, block);
    new_deposit_addresses::rollback(tx, block);
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
            address text primary key,
            cex_id int,
            spot_height int, 
            first_tx_height int
        );",
        &[],
    )?;

    // Add index on cex_id to speed up join in bootstrapping query
    tx.execute("create index on cex._bootstrapping_data (cex_id);", &[])?;

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
                -- be aware of main addresses
                left join cex.addresses mas
                    on mas.address = dif.address
                    and mas.cex_id = txs.cex_id
                -- be aware of known addresses
                    left join cex._bootstrapping_data bds
                        on bds.address = dif.address
                        and bds.cex_id = txs.cex_id
                where dif.value < 0
                    and dif.height = _height
                    -- exclude txs from main addresses
                    and mas.address is null
                    -- exclude txs from known deposit addresses
                    and bds.address is null
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

    // Copy bootstrapping data to actual tables.
    // Storing new deposit addresses directly in cex.addresses.
    // No need to buffer them in cex.new_deposit_addresses since
    // no repair events will occur during bootstrap.
    tx.execute(
        "
        insert into cex.addresses (address, cex_id, type)
        select address
        , cex_id
        , 'deposit'
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
        " alter table cex.addresses add foreign key (cex_id)
            references cex.cexs (id) on delete cascade;",
        "alter table cex.addresses alter column type set not null;",
        "create index on cex.addresses(cex_id);",
        "create index on cex.addresses(type);",
        // cex.new_deposit_addresses
        "alter table cex.new_deposit_addresses add primary key (address);",
        "alter table cex.new_deposit_addresses add foreign key (cex_id)
                references cex.cexs (id) on delete cascade;",
        "alter table cex.new_deposit_addresses alter column spot_height set not null;",
        "create index on cex.new_deposit_addresses(spot_height);",
        // cex.block_processing_log
        "alter table cex.block_processing_log add primary key (header_id);",
        "create index on cex.block_processing_log (status);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

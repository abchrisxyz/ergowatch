/// Migration 30
///
/// Add main cex address for Huobi
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        insert into cex.cexs (id, name, text_id) values
            (6, 'Huobi', 'huobi');",
        &[],
    )?;
    tx.execute(
        "
        insert into cex.main_addresses_list (cex_id, address) values
            (6, '9feMGM1qwNG8NnNuk3pz4yeCGm59s2RbjFnS7DxwUxCbzUrNnJw');",
        &[],
    )?;

    // If address doesn't exist yet, stop here as nothing else to update
    if tx
        .query_one(
            "select core.address_id('9feMGM1qwNG8NnNuk3pz4yeCGm59s2RbjFnS7DxwUxCbzUrNnJw');",
            &[],
        )
        .unwrap()
        .get::<usize, Option<i64>>(0)
        .is_none()
    {
        return Ok(());
    }

    tx.execute(
        "
        insert into cex.addresses (address_id, cex_id, type)
        select core.address_id('9feMGM1qwNG8NnNuk3pz4yeCGm59s2RbjFnS7DxwUxCbzUrNnJw')
            , 6
            , 'main';",
        &[],
    )?;

    // Temporary table with deposit addresses
    tx.execute(
        "
        create table tmp_huobi_deposit_addresses as
            with incoming_transactions as (
                select height
                    , tx_id
                from adr.erg_diffs
                where address_id = core.address_id('9feMGM1qwNG8NnNuk3pz4yeCGm59s2RbjFnS7DxwUxCbzUrNnJw')
                    and value > 0
            ), deposit_candidates as (
                select d.address_id
                    , min(d.height) as spot_height
                from adr.erg_diffs d
                join incoming_transactions t on t.height = d.height and t.tx_id = d.tx_id
                where value < 0
                group by 1
                order by 2
            )
            select c.address_id
                , c.spot_height
                , min(d.height) as first_tx_height
            from deposit_candidates c
            join adr.erg_diffs d on d.address_id = c.address_id
            group by 1, 2;",
        &[],
    )?;

    // Add deposit addresses
    tx.execute(
        "
        insert into cex.addresses (address_id, cex_id, type, spot_height)
            select address_id
                , 6 as cex_id
                , 'deposit' as type
                , spot_height
            from tmp_huobi_deposit_addresses
            order by 4, 1;",
        &[],
    )?;

    // Update cex supply.
    // Block included before repair event triggers could lead to negative
    // supply values, so we have to update it here.
    tx.execute(
        "
        with diffs as (
            select d.height
                , a.cex_id
                , coalesce(sum(d.value) filter (where a.type = 'main'), 0) as main
                , coalesce(sum(d.value) filter (where a.type = 'deposit'), 0) as deposit
            from cex.addresses a
            join adr.erg_diffs d on d.address_id = a.address_id
            where a.cex_id = 6
            group by 1, 2
        )
        insert into cex.supply (height, cex_id, main, deposit)
        select height
            , cex_id
            , sum(main) over(order by height rows between unbounded preceding and current row)
            , sum(deposit) over(order by height rows between unbounded preceding and current row)
        from diffs
        order by 1;",
        &[],
    )
    .unwrap();

    // Update invalidation heights of block processing log.
    // Has no effect other than making sure the table looks
    // as if it had been synced normally. Ensures identical
    // data across all EW instances.
    tx.execute(
        "
        with blocks as (
            select h.id as header_id
                , min(d.first_tx_height) as invalidation_height
            from tmp_huobi_deposit_addresses d
            join core.headers h on h.height  = d.spot_height
            group by 1
        )
        update cex.block_processing_log p
        set invalidation_height = b.invalidation_height
        from blocks b
        where b.header_id = p.header_id
            and b.invalidation_height < p.invalidation_height
        ",
        &[],
    )?;

    // Ensure next repair covers first tx of new main/deposit addresses.
    // This is done by marking a block with an invalidation height
    // at or prior to this migrations invalidation height.
    let repair_start_height: Option<i32> = tx.query_one("
        select least(
            (
                select min(first_tx_height)
                from tmp_huobi_deposit_addresses
            ), (
                select min(height)
                from adr.erg_diffs
                where address_id = core.address_id('9feMGM1qwNG8NnNuk3pz4yeCGm59s2RbjFnS7DxwUxCbzUrNnJw')
            )
        );", &[]).unwrap().get(0);
    if let Some(h) = repair_start_height {
        tx.execute(
            "
            update cex.block_processing_log
            set status = 'pending'
            where header_id = (
                select header_id
                from cex.block_processing_log
                where invalidation_height <= $1
                order by invalidation_height desc
                limit 1
            );",
            &[&h],
        )?;
    }

    // Cleanup temp table
    tx.execute("drop table tmp_huobi_deposit_addresses; ", &[])?;

    Ok(())
}

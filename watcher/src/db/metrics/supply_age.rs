/// Mean age of circulating supply
use crate::db::addresses;
use crate::parsing::BlockData;
use log::info;
use postgres::types::Type;
use postgres::Client;
use postgres::Transaction;
use std::time::Instant;

/*
Mean age timestamp of circulating supply can be updated incrementaly at each block like so:

Ignoring all (re)-emission addresses
    D = Sum over debits (negative diff) of diff_addr * t_addr
    C = Sum over credits (positive diff) of diff_addr * t_current_block
    CS = current circualting supply
    dt = D/CS + C/CS
    abs(D) always <= C, so dt always >= 0
    t(h) = t(h-1) + dt


However, CS is not so simple to get right because some miner contracts may contain erg
destined for reemission. So settling on simpler method for now by taking weighted average
of address mean_age_timestamp's.
*/

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    tx.execute(sql::INSERT_SNAPSHOT, &[&block.height, &block.timestamp])
        .unwrap();
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(sql::DELETE_SNAPSHOT, &[&block.height]).unwrap();
}

pub fn bootstrap(client: &mut Client, work_mem_kb: u32) -> anyhow::Result<()> {
    if !is_bootstrapped(client) {
        do_bootstrap(client, work_mem_kb)?;
    }
    if !constraints_are_set(client) {
        set_constraints(client);
    }
    Ok(())
}

fn do_bootstrap(client: &mut Client, work_mem_kb: u32) -> anyhow::Result<()> {
    info!("Bootstrapping metrics - supply age");

    let replay_id = "mtr_sa";

    // Retrieve heights to process
    let sync_height = match get_sync_height(client) {
        Some(h) => h,
        None => -1,
    };
    let blocks: Vec<(i32, i64)> = client
        .query(
            "
            select height
                , timestamp
            from core.headers
            where height > $1;",
            &[&sync_height],
        )
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();

    // Prepare replay tables
    let mut tx = client.transaction()?;
    tx.execute(&format!("set local work_mem = {};", work_mem_kb), &[])?;
    addresses::replay::cleanup(&mut tx, replay_id);
    addresses::replay::prepare(&mut tx, sync_height, replay_id);
    tx.commit()?;

    // Bootstrapping will be performed in batches of 1000
    let batch_size = 1000;
    let batches = blocks.chunks(batch_size);
    let nb_batches = batches.len();

    for (ibatch, batch_blocks) in batches.enumerate() {
        let timer = Instant::now();
        let mut tx = client.transaction()?;

        tx.execute(&format!("set local work_mem = {};", work_mem_kb), &[])?;

        // Prepare statements
        let stmt_insert_snapshot = tx.prepare_typed(
            &sql::INSERT_SNAPSHOT.replace(" adr.erg ", &format!(" {replay_id}_adr.erg ")),
            &[Type::INT4, Type::INT8],
        )?;

        for (height, timestamp) in batch_blocks {
            // step replay
            addresses::replay::step(&mut tx, *height, replay_id);

            // Insert snapshot
            tx.execute(&stmt_insert_snapshot, &[height, timestamp])?;
        }

        tx.commit()?;

        info!(
            "Bootstrapping supply age metrics - batch {} / {} (processed in {:.2}s)",
            ibatch + 1,
            nb_batches,
            timer.elapsed().as_secs_f32()
        );
    }

    // Cleanup replay tables
    let mut tx = client.transaction()?;
    addresses::replay::cleanup(&mut tx, replay_id);
    tx.commit()?;

    client.execute(
        "update mtr._log set address_counts_bootstrapped = TRUE;",
        &[],
    )?;

    Ok(())
}

fn is_bootstrapped(client: &mut Client) -> bool {
    let row = client
        .query_one("select supply_age_bootstrapped from mtr._log;", &[])
        .unwrap();
    row.get(0)
}

fn constraints_are_set(client: &mut Client) -> bool {
    let row = client
        .query_one("select supply_age_constraints_set from mtr._log;", &[])
        .unwrap();
    row.get(0)
}

/// Get sync height of address counts tables.
fn get_sync_height(client: &mut Client) -> Option<i32> {
    // P2PK and other tables are progressed in sync, so enough to probe only one.
    let row = client
        .query_one("select max(height) from mtr.supply_age;", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(client: &mut Client) {
    let statements = vec![
        "alter table mtr.supply_age add primary key(height);",
        "alter table mtr.supply_age alter column height set not null;",
        "alter table mtr.supply_age alter column secs_all set not null;",
        "alter table mtr.supply_age alter column secs_p2pk set not null;",
        "alter table mtr.supply_age alter column secs_exchanges set not null;",
        "alter table mtr.supply_age alter column secs_contracts set not null;",
        "alter table mtr.supply_age alter column secs_miners set not null;",
        "update mtr._log set supply_age_constraints_set = TRUE;",
    ];
    let mut tx = client.transaction().unwrap();
    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
    tx.commit().unwrap();
}

mod sql {
    pub(super) const INSERT_SNAPSHOT: &str = "
        insert into mtr.supply_age (
            height,
            secs_all,
            secs_p2pk,
            secs_exchanges,
            secs_contracts,
            secs_miners
    )
    with mean_age_timestamps as (
        select coalesce(sum(value::numeric * mean_age_timestamp) / sum(value), 0) as t_all
            , coalesce(sum(value::numeric * mean_age_timestamp) filter(where a.p2pk and c.address_id is null) / sum(value) filter(where a.p2pk and c.address_id is null), 0) as t_p2pk
            , coalesce(sum(value::numeric * mean_age_timestamp) filter(where a.p2pk and c.address_id is not null) / sum(value) filter(where a.p2pk and c.address_id is not null), 0) as t_cexs
            , coalesce(sum(value::numeric * mean_age_timestamp) filter(where not a.p2pk and not a.miner) / sum(value) filter(where not a.p2pk and not a.miner), 0) as t_cons
            , coalesce(sum(value::numeric * mean_age_timestamp) filter(where a.miner) / sum(value) filter(where a.miner), 0) as t_mins
        from adr.erg b
        join core.addresses a on a.id = b.address_id
        left join cex.addresses c on c.address_id = b.address_id and c.type = 'main'
        -- exclude emission and treasury contracts
        where b.address_id not in (1, 3, 596523, 599350)
    )
    select $1
        , ($2::bigint - t_all) / 1000
        , ($2::bigint - t_p2pk) / 1000
        , ($2::bigint - t_cexs) / 1000
        , ($2::bigint - t_cons) / 1000
        , ($2::bigint - t_mins) / 1000
    from mean_age_timestamps;";

    pub(super) const DELETE_SNAPSHOT: &str = "delete from mtr.supply_age where height= $1;";
}

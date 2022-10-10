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
    CS = current circulating supply
    dt = D/CS + C/CS
    abs(D) always <= C, so dt always >= 0
    t(h) = t(h-1) + dt


However, CS is not so simple to get right because some miner contracts may contain erg
destined for reemission. So settling on simpler method for now by taking weighted average
of address mean_age_timestamp's.
*/

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    tx.execute(sql::INSERT_SNAPSHOT, &[&block.height]).unwrap();

}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(sql::DELETE_SNAPSHOT_TIMESTAMPS, &[&block.height])
        .unwrap();
    tx.execute(sql::DELETE_SNAPSHOT_SECONDS, &[&block.height])
        .unwrap();
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
    // Replay prep doen't work for any h >= 0
    if sync_height == -1 {
        addresses::replay::prepare_with_age(&mut tx, sync_height, replay_id);
    } else {
        addresses::replay::resume(&mut tx, sync_height, replay_id);
    }
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
            &[Type::INT4],
        )?;

        for (height, _timestamp) in batch_blocks {
            // step replay
            addresses::replay::step_with_age(&mut tx, *height, replay_id);


            // Insert snapshot
            tx.execute(&stmt_insert_snapshot, &[height])?;
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

    client.execute("update mtr._log set supply_age_bootstrapped = TRUE;", &[])?;

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

/// Get sync height of supply age tables.
fn get_sync_height(client: &mut Client) -> Option<i32> {
    // All tables are progressed in sync, so enough to probe only one.
    let row = client
        .query_one("select max(height) from mtr.supply_age_timestamps;", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(client: &mut Client) {
    let statements = vec![
        "alter table mtr.supply_age_timestamps add primary key(height);",
        "alter table mtr.supply_age_timestamps alter column height set not null;",
        "alter table mtr.supply_age_timestamps alter column overall set not null;",
        "alter table mtr.supply_age_timestamps alter column p2pks set not null;",
        "alter table mtr.supply_age_timestamps alter column cexs set not null;",
        "alter table mtr.supply_age_timestamps alter column contracts set not null;",
        "alter table mtr.supply_age_timestamps alter column miners set not null;",
        "alter table mtr.supply_age_seconds add primary key(height);",
        "alter table mtr.supply_age_seconds alter column height set not null;",
        "alter table mtr.supply_age_seconds alter column overall set not null;",
        "alter table mtr.supply_age_seconds alter column p2pks set not null;",
        "alter table mtr.supply_age_seconds alter column cexs set not null;",
        "alter table mtr.supply_age_seconds alter column contracts set not null;",
        "alter table mtr.supply_age_seconds alter column miners set not null;",
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
        insert into mtr.supply_age_timestamps (
            height,
            overall,
            p2pks,
            cexs,
            contracts,
            miners
        )
        with mean_age_timestamps as (
            select coalesce(sum(value::numeric * mean_age_timestamp) / sum(value), 0) as t_all
                , coalesce(
                    sum(value::numeric * mean_age_timestamp) filter(where a.p2pk and c.address_id is null)
                    / sum(value) filter(where a.p2pk and c.address_id is null), 0
                ) as t_p2pk
                , coalesce(
                    sum(value::numeric * mean_age_timestamp) filter(where a.p2pk and c.address_id is not null)
                    / sum(value) filter(where a.p2pk and c.address_id is not null), 0
                ) as t_cexs
                , coalesce(
                    sum(value::numeric * mean_age_timestamp) filter(where not a.p2pk and not a.miner)
                    / sum(value) filter(where not a.p2pk and not a.miner), 0
                ) as t_cons
                , coalesce(
                    sum(value::numeric * mean_age_timestamp) filter(where a.miner)
                    / sum(value) filter(where a.miner), 0
                ) as t_mins
            from adr.erg b
            join core.addresses a on a.id = b.address_id
            left join cex.addresses c on c.type = 'main' and c.address_id = b.address_id
            -- exclude emission and treasury contracts
            where b.address_id <> core.address_id('2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU')
                and b.address_id <> coalesce(core.address_id('22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77'), 0)
                and b.address_id <> coalesce(core.address_id('6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p'), 0)
                and b.address_id <> coalesce(core.address_id('4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy'), 0)
                -- exclude fees as always zero
                and b.address_id <> coalesce(core.address_id('2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe'), 0)
        )
        select $1
            , t_all::bigint
            , t_p2pk::bigint
            , t_cexs::bigint
            , t_cons::bigint
            , t_mins::bigint
        from mean_age_timestamps;";

    pub(super) const DELETE_SNAPSHOT_TIMESTAMPS: &str =
        "delete from mtr.supply_age_timestamps where height= $1;";
    pub(super) const DELETE_SNAPSHOT_SECONDS: &str =
        "delete from mtr.supply_age_seconds where height= $1;";
}

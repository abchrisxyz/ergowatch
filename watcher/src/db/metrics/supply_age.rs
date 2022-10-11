// use super::supply_composition::Cache as SupplyCompositionCache;
/// Mean age of circulating supply
use crate::db::addresses;
use crate::parsing::BlockData;
use log::info;
use postgres::types::Type;
use postgres::Client;
use postgres::Transaction;
use rust_decimal::Decimal;
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
*/

pub(super) fn include(tx: &mut Transaction, block: &BlockData, sad: &SupplyAgeDiffs) {
    tx.execute(
        sql::APPEND_SNAPSHOT,
        &[
            &block.height,
            &sad.p2pks,
            &sad.cexs,
            &sad.contracts,
            &sad.miners,
        ],
    )
    .unwrap();
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

    let mut blocks: Vec<i32> = client
        .query(
            "
            select height
            from core.headers
            where height > $1;",
            &[&sync_height],
        )
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();

    // Prepare replay tables
    let mut tx = client.transaction()?;
    tx.execute(&format!("set local work_mem = {};", work_mem_kb), &[])?;
    // Replay prep doesn't work for any h >= 0
    if sync_height == -1 {
        addresses::replay::prepare_with_age(&mut tx, sync_height, replay_id);
    } else {
        addresses::replay::resume(&mut tx, sync_height, replay_id);
    }
    tx.commit()?;

    // If starting from scratch (i.e. not resuming a previous bootstrap session)
    // then handle first height separately:
    if sync_height == -1 {
        let first_height = blocks[0];
        let mut tx = client.transaction()?;
        addresses::replay::step_with_age(&mut tx, first_height, replay_id);
        tx
            .execute(
                "
                insert into mtr.supply_age_timestamps (height, overall, p2pks, cexs, contracts, miners)
                values ($1, 0, 0, 0, 0, 0);",
                &[&first_height],
            )
            .unwrap();
        tx.commit().unwrap();
        blocks.remove(0);
    }

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
            &sql::APPEND_SNAPSHOT.replace(" adr.erg ", &format!(" {replay_id}_adr.erg ")),
            &[
                Type::INT4,
                Type::NUMERIC,
                Type::NUMERIC,
                Type::NUMERIC,
                Type::NUMERIC,
            ],
        )?;

        for height in batch_blocks {
            // step replay
            let sad = addresses::replay::step_with_age(&mut tx, *height, replay_id);

            // Insert snapshot
            tx.execute(
                &stmt_insert_snapshot,
                &[height, &sad.p2pks, &sad.cexs, &sad.contracts, &sad.miners],
            )?;
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

// struct Record {
//     pub secs_all: i64,
//     pub secs_p2pk: i64,
//     pub secs_cexs: i64,
//     pub secs_contracts_: i64,
//     pub secs_miners_: i64,
// }

// impl From<Row> for Record {
//     fn from(row: Row) -> Record {
//         Record {
//             secs_: row.get(0),
//             cex_main: row.get(1),
//             cex_deposits: row.get(2),
//             contracts: row.get(3),
//             miners: row.get(4),
//             treasury: row.get(5),
//         }
//     }
// }

#[derive(Debug)]
pub struct SupplyAgeDiffs {
    pub p2pks: Decimal,
    pub cexs: Decimal,
    pub contracts: Decimal,
    pub miners: Decimal,
}

impl SupplyAgeDiffs {
    pub fn new() -> Self {
        Self {
            p2pks: 0.into(),
            cexs: 0.into(),
            contracts: 0.into(),
            miners: 0.into(),
        }
    }

    pub fn get(tx: &mut Transaction, height: i32, timestamp: i64) -> Self {
        let row = tx
            .query_one(sql::GET_RAW_DIFFS, &[&height, &timestamp])
            .unwrap();
        Self {
            p2pks: row.get(0),
            cexs: row.get(1),
            contracts: row.get(2),
            miners: row.get(3),
        }
    }

    pub fn get_mtr_sa(tx: &mut Transaction, height: i32, timestamp: i64) -> Self {
        let row = tx
            .query_one(sql::GET_RAW_DIFFS_MTR_SA, &[&height, &timestamp])
            .unwrap();
        Self {
            p2pks: row.get(0),
            cexs: row.get(1),
            contracts: row.get(2),
            miners: row.get(3),
        }
    }
}

mod sql {
    pub(super) const DELETE_SNAPSHOT_TIMESTAMPS: &str =
        "delete from mtr.supply_age_timestamps where height= $1;";
    pub(super) const DELETE_SNAPSHOT_SECONDS: &str =
        "delete from mtr.supply_age_seconds where height= $1;";

    /// Unscaled age differences
    ///
    /// $1: height of target block
    /// $2: timestamp (ms) of target block
    pub(super) const GET_RAW_DIFFS: &str= "
        select
            -- p2pks (incl cex deposits)
            coalesce(sum(d.value) filter (where d.value > 0 and a.p2pk and c.address_id is null), 0) * $2::bigint
            + coalesce(sum(d.value::numeric * b.mean_age_timestamp) filter (where d.value < 0 and a.p2pk and c.address_id is null), 0)
            -- cexs main
            , coalesce(sum(d.value) filter (where d.value > 0 and a.p2pk and c.address_id is not null), 0) * $2::bigint
            + coalesce(sum(d.value::numeric * b.mean_age_timestamp) filter (where d.value < 0 and a.p2pk and c.address_id is not null), 0)
            -- contracts
            , coalesce(sum(d.value) filter (where d.value > 0 and  not a.p2pk and not a.miner), 0) * $2::bigint
            + coalesce(sum(d.value::numeric * b.mean_age_timestamp) filter (where d.value < 0 and not a.p2pk and not a.miner), 0)
            -- miners
            , coalesce(sum(d.value) filter (where d.value > 0 and  a.miner), 0) * $2::bigint
            + coalesce(sum(d.value::numeric * b.mean_age_timestamp) filter (where d.value < 0 and a.miner), 0)
        from adr.erg_diffs d
        left join adr.erg b on b.address_id = d.address_id
        join core.addresses a on a.id = d.address_id
        left join cex.addresses c on c.address_id = d.address_id and c.type='main'
        where d.height = $1
            -- exclude emission contracts
            and d.address_id <> core.address_id('2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU')
            and d.address_id <> coalesce(core.address_id('22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77'), 0)
            and d.address_id <> coalesce(core.address_id('6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p'), 0)
            -- exclude treasury
            and d.address_id <> core.address_id('4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy')
            -- exclude fee contract as always net zero
            and d.address_id <> coalesce(core.address_id('2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe'), 0)
        ;
        ";

    /// Unscaled age differences, from replay balances
    ///
    /// $1: height of target block
    /// $2: timestamp (ms) of target block
    pub(super) const GET_RAW_DIFFS_MTR_SA: &str= "
        select
            -- p2pks (incl cex deposits)
            coalesce(sum(d.value) filter (where d.value > 0 and a.p2pk and c.address_id is null), 0) * $2::bigint
            + coalesce(sum(d.value::numeric * b.mean_age_timestamp) filter (where d.value < 0 and a.p2pk and c.address_id is null), 0)
            -- cexs main
            , coalesce(sum(d.value) filter (where d.value > 0 and a.p2pk and c.address_id is not null), 0) * $2::bigint
            + coalesce(sum(d.value::numeric * b.mean_age_timestamp) filter (where d.value < 0 and a.p2pk and c.address_id is not null), 0)
            -- contracts
            , coalesce(sum(d.value) filter (where d.value > 0 and  not a.p2pk and not a.miner), 0) * $2::bigint
            + coalesce(sum(d.value::numeric * b.mean_age_timestamp) filter (where d.value < 0 and not a.p2pk and not a.miner), 0)
            -- miners
            , coalesce(sum(d.value) filter (where d.value > 0 and  a.miner), 0) * $2::bigint
            + coalesce(sum(d.value::numeric * b.mean_age_timestamp) filter (where d.value < 0 and a.miner), 0)
        from adr.erg_diffs d
        left join mtr_sa_adr.erg b on b.address_id = d.address_id
        join core.addresses a on a.id = d.address_id
        left join cex.addresses c on c.address_id = d.address_id and c.type='main'
        where d.height = $1
            -- exclude emission contracts
            and d.address_id <> core.address_id('2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU')
            and d.address_id <> coalesce(core.address_id('22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77'), 0)
            and d.address_id <> coalesce(core.address_id('6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p'), 0)
            -- exclude treasury
            and d.address_id <> core.address_id('4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy')
            -- exclude fee contract as always net zero
            and d.address_id <> coalesce(core.address_id('2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe'), 0)
        ;
        ";

    /// Insert new snapshot for given height.
    ///
    /// $1: height of target block
    /// $2: p2pk diffs
    /// $3: main cex diffs
    /// $4: contract diffs
    /// $5: miner diffs
    ///
    /// Assumes balances represent state at `height`.
    pub(super) const APPEND_SNAPSHOT: &str = "
        with new_age_timestamps as (
            select
                -- p2pks (incl cex deposits)
                case when s.p2pks + s.cex_deposits > 0 then
                    (coalesce(prev_t.p2pks, 0) * (prev_s.p2pks + prev_s.cex_deposits) + $2::numeric)
                    / (s.p2pks + s.cex_deposits)
                else 0
                end as p2pks
                
                -- cexs main
                , case when s.cex_main > 0 then
                    (coalesce(prev_t.cexs, 0) * prev_s.cex_main + $3::numeric ) / s.cex_main
                else 0
                end as cexs
                
                -- contracts
                , case when s.contracts > 0 then
                    (coalesce(prev_t.contracts, 0) * prev_s.contracts + $4::numeric) / s.contracts
                else 0
                end as contracts

                -- miners
                , case when s.miners > 0 then
                    (coalesce(prev_t.miners, 0) * prev_s.miners + $5::numeric) / s.miners
                else 0
                end as miners
    
            from mtr.supply_composition s
            join mtr.supply_composition prev_s on prev_s.height = $1 - 1
            left join mtr.supply_age_timestamps prev_t on prev_t.height = $1 - 1 
            where s.height = $1
        )
        insert into mtr.supply_age_timestamps (height, overall, p2pks, cexs, contracts, miners)
        select $1
            , (
                (n.p2pks * (s.p2pks + s.cex_deposits)
                    + n.cexs * s.cex_main
                    + n.contracts * s.contracts
                    + n.miners * s.miners
                ) / (s.p2pks + s.cex_main + s.cex_deposits + s.contracts + s.miners)
            )::bigint
            , n.p2pks::bigint
            , n.cexs::bigint
            , n.contracts::bigint
            , n.miners::bigint
        from new_age_timestamps n
        join mtr.supply_composition s on s.height = $1;
        ";
}

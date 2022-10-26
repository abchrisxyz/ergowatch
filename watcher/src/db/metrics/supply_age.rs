/// Mean age of emitted supply
use super::heights::Cache as HeightsCache;
use super::supply_composition::Cache as SupplyCompositionCache;
use super::supply_composition::Record as SupplyCompositionRecord;
use super::utils::bootstrap_change_summary;
use super::utils::refresh_change_summary;
use crate::db::addresses;
use crate::parsing::BlockData;
use log::info;
use postgres::types::Type;
use postgres::Client;
use postgres::GenericClient;
use postgres::Transaction;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
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

const SUMMARY_COLUMNS: &[&'static str] = &["overall", "p2pks", "cexs", "contracts", "miners"];

pub(super) fn include(
    tx: &mut Transaction,
    block: &BlockData,
    age_cache: &mut Cache,
    sc_cache: &SupplyCompositionCache,
    sad: &SupplyAgeDiffs,
) {
    // Decimal version of age timestamps
    let t_p2pks = Decimal::from_i64(age_cache.timestamps.p2pks).unwrap();
    let t_cexs = Decimal::from_i64(age_cache.timestamps.cexs).unwrap();
    let t_cons = Decimal::from_i64(age_cache.timestamps.contracts).unwrap();
    let t_miners = Decimal::from_i64(age_cache.timestamps.miners).unwrap();

    // Decimal version of supply composition
    let sc = sc_cache.curr.composition;
    let s_p2pks = Decimal::from_i64(sc.p2pks + sc.cex_deposits).unwrap();
    let s_cexs = Decimal::from_i64(sc.cex_main).unwrap();
    let s_cons = Decimal::from_i64(sc.contracts).unwrap();
    let s_miners = Decimal::from_i64(sc.miners).unwrap();

    // Decimal version of previous supply composition
    let sc = sc_cache.prev.composition;
    let prev_s_p2pks = Decimal::from_i64(sc.p2pks + sc.cex_deposits).unwrap();
    let prev_s_cexs = Decimal::from_i64(sc.cex_main).unwrap();
    let prev_s_cons = Decimal::from_i64(sc.contracts).unwrap();
    let prev_s_miners = Decimal::from_i64(sc.miners).unwrap();

    let p2pks = progress_timestamp(t_p2pks, s_p2pks, prev_s_p2pks, sad.p2pks);
    let cexs = progress_timestamp(t_cexs, s_cexs, prev_s_cexs, sad.cexs);
    let cons = progress_timestamp(t_cons, s_cons, prev_s_cons, sad.contracts);
    let miners = progress_timestamp(t_miners, s_miners, prev_s_miners, sad.miners);

    let supply = s_p2pks + s_cexs + s_cons + s_miners;
    let overall: Decimal = p2pks / supply * s_p2pks
        + cexs / supply * s_cexs
        + cons / supply * s_cons
        + miners / supply * s_miners;

    let timestamps = Timestamps {
        overall: overall.round().to_i64().unwrap(),
        p2pks: p2pks.round().to_i64().unwrap(),
        cexs: cexs.round().to_i64().unwrap(),
        contracts: cons.round().to_i64().unwrap(),
        miners: miners.round().to_i64().unwrap(),
    };

    tx.execute(
        sql::INSERT_TIMESTAMPS_SNAPSHOT,
        &[
            &block.height,
            &timestamps.overall,
            &timestamps.p2pks,
            &timestamps.cexs,
            &timestamps.contracts,
            &timestamps.miners,
        ],
    )
    .unwrap();

    tx.execute(
        sql::APPEND_DAYS_SNAPSHOT,
        &[&block.height, &block.timestamp],
    )
    .unwrap();

    // Update cache
    age_cache.height = block.height;
    age_cache.timestamps = timestamps;
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    tx.execute(sql::DELETE_SNAPSHOT_TIMESTAMPS, &[&block.height])
        .unwrap();
    tx.execute(sql::DELETE_SNAPSHOT_DAYS, &[&block.height])
        .unwrap();

    // Update cache
    let new_cache = Cache::load(tx);
    cache.height = new_cache.height;
    cache.timestamps = new_cache.timestamps;
    assert_eq!(cache.height, block.height - 1);
}

pub(super) fn refresh_summary(tx: &mut Transaction, hc: &HeightsCache) {
    refresh_change_summary(tx, hc, "mtr.supply_age_days", &SUMMARY_COLUMNS);
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

    let mut blocks: Vec<(i32, i64)> = client
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
        let first_height = blocks[0].0;
        let first_ts = blocks[0].1;
        let mut tx = client.transaction()?;
        addresses::replay::step_with_age(&mut tx, first_height, first_ts, replay_id);
        tx
            .execute(
                "
                insert into mtr.supply_age_timestamps (height, overall, p2pks, cexs, contracts, miners)
                values ($1, 0, 0, 0, 0, 0);",
                &[&first_height],
            )
            .unwrap();
        tx.execute(
            "
                insert into mtr.supply_age_days (height, overall, p2pks, cexs, contracts, miners)
                values ($1, 0., 0., 0., 0., 0.);",
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

    // Init cache
    let mut timestamps = Cache::load(client).timestamps;

    for (ibatch, batch_blocks) in batches.enumerate() {
        let timer = Instant::now();
        let mut tx = client.transaction()?;

        tx.execute(&format!("set local work_mem = {};", work_mem_kb), &[])?;

        // Prepare statements
        let stmt_insert_snapshot = tx.prepare_typed(
            &sql::INSERT_TIMESTAMPS_SNAPSHOT,
            &[
                Type::INT4,
                Type::INT8,
                Type::INT8,
                Type::INT8,
                Type::INT8,
                Type::INT8,
            ],
        )?;

        // Preload supply composition data
        // For each height to be boostrapped, need record at h and h-1, so start at h-1
        let first_h: i32 = batch_blocks.first().unwrap().0 - 1;
        let last_h: i32 = batch_blocks.last().unwrap().0;
        let sc_recs = SupplyCompositionRecord::get_range(&mut tx, first_h, last_h);
        assert_eq!(sc_recs[0].height, first_h);
        assert_eq!(sc_recs.len() as i32, last_h - first_h + 1);

        for (i, (height, timestamp)) in batch_blocks.iter().enumerate() {
            // step replay
            let sad = addresses::replay::step_with_age(&mut tx, *height, *timestamp, replay_id);

            // Supply composition
            let sc_prev = sc_recs[i];
            let sc_curr = sc_recs[i + 1];

            // p2pks (including cex deposit addresses)
            let p2pks: Decimal = progress_timestamp_i64(
                timestamps.p2pks,
                sc_curr.composition.p2pks + sc_curr.composition.cex_deposits,
                sc_prev.composition.p2pks + sc_prev.composition.cex_deposits,
                sad.p2pks,
            );

            // Main exchage addresses
            let cexs: Decimal = progress_timestamp_i64(
                timestamps.cexs,
                sc_curr.composition.cex_main,
                sc_prev.composition.cex_main,
                sad.cexs,
            );

            // Contracts
            let cons: Decimal = progress_timestamp_i64(
                timestamps.contracts,
                sc_curr.composition.contracts,
                sc_prev.composition.contracts,
                sad.contracts,
            );

            // Miners
            let miners: Decimal = progress_timestamp_i64(
                timestamps.miners,
                sc_curr.composition.miners,
                sc_prev.composition.miners,
                sad.miners,
            );

            // Overall from other terms
            let p2pk_supply =
                Decimal::from_i64(sc_curr.composition.p2pks + sc_curr.composition.cex_deposits)
                    .unwrap();
            let cexs_supply = Decimal::from_i64(sc_curr.composition.cex_main).unwrap();
            let cons_supply = Decimal::from_i64(sc_curr.composition.contracts).unwrap();
            let mins_supply = Decimal::from_i64(sc_curr.composition.miners).unwrap();
            let supply = p2pk_supply + cexs_supply + cons_supply + mins_supply;
            let overall = p2pks / supply * p2pk_supply
                + cexs / supply * cexs_supply
                + cons / supply * cons_supply
                + miners / supply * mins_supply;

            // Update cache
            timestamps.overall = overall.round().to_i64().unwrap();
            timestamps.p2pks = p2pks.round().to_i64().unwrap();
            timestamps.cexs = cexs.round().to_i64().unwrap();
            timestamps.contracts = cons.round().to_i64().unwrap();
            timestamps.miners = miners.round().to_i64().unwrap();

            tx.execute(
                &stmt_insert_snapshot,
                &[
                    &height,
                    &timestamps.overall,
                    &timestamps.p2pks,
                    &timestamps.cexs,
                    &timestamps.contracts,
                    &timestamps.miners,
                ],
            )
            .unwrap();
        }

        tx.commit()?;

        info!(
            "Bootstrapping supply age metrics - batch {} / {} (processed in {:.2}s)",
            ibatch + 1,
            nb_batches,
            timer.elapsed().as_secs_f32()
        );
    }

    // Add snapshots in days
    info!("Bootstrapping supply age metrics - calculating age in days");
    client
        .execute(
            "
            insert into mtr.supply_age_days (height, overall, p2pks, cexs, contracts, miners)
                select h.height
                    , ((h.timestamp - t.overall) / 86400000.)::real
                    , ((h.timestamp - t.p2pks) / 86400000.)::real
                    , ((h.timestamp - t.cexs) / 86400000.)::real
                    , ((h.timestamp - t.contracts) / 86400000.)::real
                    , ((h.timestamp - t.miners) / 86400000.)::real
                from mtr.supply_age_timestamps t
                join core.headers h on h.height = t.height
                left join mtr.supply_age_days d on d.height = t.height
                where d.height is null
                order by 1;
            ;",
            &[],
        )
        .unwrap();

    // Summary tables
    let mut tx = client.transaction()?;
    bootstrap_change_summary(&mut tx, "mtr.supply_age_days", &SUMMARY_COLUMNS);

    // Cleanup replay tables
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
        "alter table mtr.supply_age_days add primary key(height);",
        "alter table mtr.supply_age_days alter column height set not null;",
        "alter table mtr.supply_age_days alter column overall set not null;",
        "alter table mtr.supply_age_days alter column p2pks set not null;",
        "alter table mtr.supply_age_days alter column cexs set not null;",
        "alter table mtr.supply_age_days alter column contracts set not null;",
        "alter table mtr.supply_age_days alter column miners set not null;",
        "update mtr._log set supply_age_constraints_set = TRUE;",
    ];
    let mut tx = client.transaction().unwrap();
    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
    tx.commit().unwrap();
}

/// Calculate new age timestamp
///
/// `ts`: last age timestamp to be updated
/// `supply`: current supply
/// `prev_supply`: previous supply
/// `diff`: raw age diff (i.e. nanoERG*ms)
fn progress_timestamp(
    ts: Decimal,
    supply: Decimal,
    prev_supply: Decimal,
    diff: Decimal,
) -> Decimal {
    if supply > Decimal::ZERO {
        (ts * prev_supply + diff) / supply
    } else {
        Decimal::ZERO
    }
}

fn progress_timestamp_i64(ts: i64, supply: i64, prev_supply: i64, diff: Decimal) -> Decimal {
    if supply > 0 {
        let ts = Decimal::from_i64(ts).unwrap();
        let prev_supply = Decimal::from_i64(prev_supply).unwrap();
        let supply = Decimal::from_i64(supply).unwrap();
        (ts * prev_supply + diff) / supply
    } else {
        Decimal::ZERO
    }
}

#[derive(Debug)]
pub struct Cache {
    pub height: i32,
    timestamps: Timestamps,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            height: 0,
            timestamps: Timestamps::new(),
        }
    }

    pub(super) fn load(client: &mut impl GenericClient) -> Self {
        match client
            .query_opt(sql::GET_LATEST_TIMESTAMPS_RECORD, &[])
            .unwrap()
        {
            Some(row) => Self {
                height: row.get(0),
                timestamps: Timestamps {
                    overall: row.get(1),
                    p2pks: row.get(2),
                    cexs: row.get(3),
                    contracts: row.get(4),
                    miners: row.get(5),
                },
            },
            None => Self::new(),
        }
    }
}

#[derive(Debug)]
struct Timestamps {
    pub overall: i64,
    pub p2pks: i64,
    pub cexs: i64,
    pub contracts: i64,
    pub miners: i64,
}

impl Timestamps {
    pub fn new() -> Self {
        Self {
            overall: 0,
            p2pks: 0,
            cexs: 0,
            contracts: 0,
            miners: 0,
        }
    }
}

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
    /// Latest row in mtr.supply_age_timestamps
    pub(super) const GET_LATEST_TIMESTAMPS_RECORD: &str = "
        select height
            , overall
            , p2pks
            , cexs
            , contracts
            , miners
        from mtr.supply_age_timestamps
        order by height desc limit 1;";

    /// Unscaled age differences in ms
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

    /// Unscaled age differences in ms, from replay balances
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

    /// Insert new record
    ///
    /// $1: height of target block
    /// $2: overall diffs
    /// $3: p2pk diffs
    /// $4: main cex diffs
    /// $5: contract diffs
    /// $6: miner diffs
    ///
    /// Assumes balances represent state at `height`.
    pub(super) const INSERT_TIMESTAMPS_SNAPSHOT: &str = "
        insert into mtr.supply_age_timestamps (height, overall, p2pks, cexs, contracts, miners)
        values ($1, $2, $3, $4, $5, $6);";

    /// Adds a record mtr.supply_age_days
    ///
    /// $1: height of target block
    /// $2: timestamp of target block
    ///
    /// Assumes mtr.supply_age_days has a record for `height`.
    pub(super) const APPEND_DAYS_SNAPSHOT: &str = "
        insert into mtr.supply_age_days (height, overall, p2pks, cexs, contracts, miners)
        select $1
            , (($2 - overall) / 86400000.)::real
            , (($2 - p2pks) / 86400000.)::real
            , (($2 - cexs) / 86400000.)::real
            , (($2 - contracts) / 86400000.)::real
            , (($2 - miners) / 86400000.)::real
        from mtr.supply_age_timestamps
        where height = $1;
        ";

    pub(super) const DELETE_SNAPSHOT_TIMESTAMPS: &str =
        "delete from mtr.supply_age_timestamps where height= $1;";
    pub(super) const DELETE_SNAPSHOT_DAYS: &str =
        "delete from mtr.supply_age_days where height= $1;";
}

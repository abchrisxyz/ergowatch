//! # metrics
//!
//! Process blocks into metrics over time.
mod address_counts;
mod cexs;
mod ergusd;
mod supply_age;
mod supply_composition;
mod supply_distribution;
mod transactions;
pub mod utxos;
mod volume;
use crate::db::coingecko::Cache as CoinGeckoCache;
use crate::parsing::BlockData;
use postgres::Client;
use postgres::Transaction;

pub(super) fn include_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
    cgo_cache: &CoinGeckoCache,
) -> anyhow::Result<()> {
    cache.height_1d_ago = get_height_days_ago(tx, 1, block.timestamp, cache.height_1d_ago);
    cache.height_7d_ago = get_height_days_ago(tx, 7, block.timestamp, cache.height_7d_ago);
    cache.height_28d_ago = get_height_days_ago(tx, 28, block.timestamp, cache.height_28d_ago);
    ergusd::include(tx, block, &mut cache.ergusd, cgo_cache);
    utxos::include(tx, block, cache);
    cexs::include(tx, block);
    address_counts::include(tx, block, &mut cache.address_counts);
    supply_composition::include(tx, block, &mut cache.supply_composition);
    supply_age::include(tx, block);
    supply_distribution::include(tx, block, &cache.address_counts);
    transactions::include(tx, block, cache);
    volume::include(tx, block, cache);

    if ergusd::pending_update(&cache.ergusd, cgo_cache) {
        // Update ergusd values
        ergusd::update_provisional_values(tx, &mut cache.ergusd)
        // TODO: Update dependents (none yet)
    }
    Ok(())
}

pub(super) fn rollback_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    volume::rollback(tx, block);
    transactions::rollback(tx, block);
    supply_distribution::rollback(tx, block);
    supply_age::rollback(tx, block);
    supply_composition::rollback(tx, block, &mut cache.supply_composition);
    address_counts::rollback(tx, block, &mut cache.address_counts);
    cexs::rollback(tx, block);
    utxos::rollback(tx, block, cache);
    ergusd::rollback(tx, block, &mut cache.ergusd);
    cache.height_1d_ago = load_height_days_ago_at(tx, 1, block.height - 1);
    cache.height_7d_ago = load_height_days_ago_at(tx, 7, block.height - 1);
    cache.height_28d_ago = load_height_days_ago_at(tx, 28, block.height - 1);
    Ok(())
}

pub(super) fn bootstrap(client: &mut Client, work_mem_kb: u32) -> anyhow::Result<()> {
    let mut tx = client.transaction()?;
    ergusd::bootstrap(&mut tx)?;
    utxos::bootstrap(&mut tx)?;
    cexs::bootstrap(&mut tx)?;
    tx.commit()?;

    address_counts::bootstrap(client, work_mem_kb)?;
    supply_composition::bootstrap(client, work_mem_kb)?;
    supply_age::bootstrap(client, work_mem_kb)?;
    supply_distribution::bootstrap(client, work_mem_kb)?;
    transactions::bootstrap(client, work_mem_kb)?;
    volume::bootstrap(client, work_mem_kb)?;
    Ok(())
}

#[derive(Debug)]
pub struct Cache {
    pub address_counts: address_counts::Cache,
    pub ergusd: ergusd::Cache,
    pub supply_composition: supply_composition::Cache,
    pub utxos: i64,
    // Heights x days prior to current last block
    height_1d_ago: i32,
    height_7d_ago: i32,
    height_28d_ago: i32,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            address_counts: address_counts::Cache::new(),
            ergusd: ergusd::Cache::new(),
            supply_composition: supply_composition::Cache::new(),
            utxos: 0,
            height_1d_ago: 0,
            height_7d_ago: 0,
            height_28d_ago: 0,
        }
    }

    pub fn load(client: &mut Client) -> Self {
        Self {
            address_counts: address_counts::Cache::load(client),
            ergusd: ergusd::Cache::load(client),
            supply_composition: supply_composition::Cache::load(client),
            utxos: utxos::get_utxo_count(client),
            height_1d_ago: load_height_days_ago(client, 1),
            height_7d_ago: load_height_days_ago(client, 7),
            height_28d_ago: load_height_days_ago(client, 28),
        }
    }
}

pub(super) fn repair(tx: &mut Transaction, height: i32) {
    cexs::repair(tx, height);
    supply_composition::repair(tx, height);
    supply_distribution::repair(tx, height);
}

/// Return height of first block in `days` days window since timestamp of last block
fn load_height_days_ago(client: &mut Client, days: i64) -> i32 {
    match client
        .query_opt(
            "
            select height
            from core.headers
            where timestamp > (
                select timestamp - 86400000::bigint * $1
                from core.headers
                order by height desc
                limit 1
            )
            order by height
            limit 1;
        ",
            &[&days],
        )
        .unwrap()
    {
        Some(row) => row.get(0),
        None => 0,
    }
}

/// Return height of first block in `days` days window since timestamp of block at `height`
fn load_height_days_ago_at(tx: &mut Transaction, days: i64, height: i32) -> i32 {
    tx.query_one(
        "
            select height
            from core.headers
            where timestamp > (
                select timestamp - 86400000::bigint * $1
                from core.headers
                where height = $2
                order by height desc
                limit 1
            )
            order by height
            limit 1;
        ",
        &[&days, &height],
    )
    .unwrap()
    .get(0)
}

/// Return height of first block in `days` day window prior to `timestamp`
///
/// `days`: time window size in days
/// `timestamp`: timestamp of current block (i.e. end of target 24h window)
/// `prev_value`: start height of previous 24h window
fn get_height_days_ago(tx: &mut Transaction, days: i64, timestamp: i64, prev_value: i32) -> i32 {
    tx.query_one(
        "
        select height
        from core.headers
        where height >= $2
            and timestamp > $3 - 86400000::bigint * $1
        order by height
        limit 1;
    ",
        &[&days, &prev_value, &timestamp],
    )
    .unwrap()
    .get(0)
}

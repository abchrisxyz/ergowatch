//! # metrics
//!
//! Process blocks into metrics over time.
pub mod utxos;

use crate::parsing::BlockData;
use crate::session::cache;
use log::debug;
use postgres::Transaction;

pub(super) fn include_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut cache::Metrics,
) -> anyhow::Result<()> {
    utxos::include(tx, block, cache);
    Ok(())
}

pub(super) fn rollback_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut cache::Metrics,
) -> anyhow::Result<()> {
    utxos::rollback(tx, block, cache);
    Ok(())
}

pub(super) fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    utxos::bootstrap(tx)?;
    Ok(())
}

pub fn load_cache(client: &mut postgres::Client) -> cache::Metrics {
    debug!("Loading metrics cache");

    let any_metrics: bool = client
        .query_one("select exists (select height from mtr.utxos);", &[])
        .unwrap()
        .get(0);

    if !any_metrics {
        return cache::Metrics::new();
    }

    let utxos: i64 = client
        .query_one(utxos::SELECT_LAST_SNAPSHOT_VALUE, &[])
        .unwrap()
        .get(0);

    cache::Metrics { utxos: utxos }
}

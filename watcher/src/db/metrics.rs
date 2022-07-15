//! # metrics
//!
//! Process blocks into metrics over time.
mod cexs;
pub mod utxos;
use crate::parsing::BlockData;
use log::debug;
use postgres::Client;
use postgres::Transaction;

pub(super) fn include_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    utxos::include(tx, block, cache);
    cexs::include(tx, block);
    Ok(())
}

pub(super) fn rollback_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    utxos::rollback(tx, block, cache);
    cexs::rollback(tx, block);
    Ok(())
}

pub(super) fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    utxos::bootstrap(tx)?;
    cexs::bootstrap(tx)?;
    Ok(())
}

#[derive(Debug)]
pub struct Cache {
    pub utxos: i64,
}

impl Cache {
    pub fn new() -> Self {
        Self { utxos: 0 }
    }

    pub fn load(client: &mut Client) -> Self {
        debug!("Loading metrics cache");
        let any_metrics: bool = client
            .query_one("select exists (select height from mtr.utxos);", &[])
            .unwrap()
            .get(0);
        if !any_metrics {
            return Cache::new();
        }
        let utxos: i64 = client
            .query_one(utxos::SELECT_LAST_SNAPSHOT_VALUE, &[])
            .unwrap()
            .get(0);
        Cache { utxos: utxos }
    }
}

pub(super) fn repair(tx: &mut Transaction, height: i32) {
    cexs::repair(tx, height);
}

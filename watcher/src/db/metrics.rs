//! # metrics
//!
//! Process blocks into metrics over time.
mod cexs;
mod ergusd;
pub mod utxos;
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
    ergusd::include(tx, block, &mut cache.ergusd, cgo_cache);
    utxos::include(tx, block, cache);
    cexs::include(tx, block);

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
    cexs::rollback(tx, block);
    utxos::rollback(tx, block, cache);
    ergusd::rollback(tx, block, &mut cache.ergusd);
    Ok(())
}

pub(super) fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    ergusd::bootstrap(tx)?;
    utxos::bootstrap(tx)?;
    cexs::bootstrap(tx)?;
    Ok(())
}

#[derive(Debug)]
pub struct Cache {
    pub ergusd: ergusd::Cache,
    pub utxos: i64,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            ergusd: ergusd::Cache::new(),
            utxos: 0,
        }
    }

    pub fn load(client: &mut Client) -> Self {
        Self {
            ergusd: ergusd::Cache::load(client),
            utxos: utxos::get_utxo_count(client),
        }
    }
}

pub(super) fn repair(tx: &mut Transaction, height: i32) {
    cexs::repair(tx, height);
}

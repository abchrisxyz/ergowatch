//! # metrics
//!
//! Process blocks into metrics over time.
pub mod utxos;

use super::SQLStatement;
use crate::parsing::BlockData;
use crate::session::cache;
use log::debug;

pub fn prep(block: &BlockData, cache: &mut cache::Metrics) -> Vec<SQLStatement> {
    // let mut sql_statements: Vec<SQLStatement> = Vec::new();
    let height = block.height;

    // New value is cached value plus diff
    cache.utxos += block_utxo_diff(block);

    vec![utxos::insert_snapshot(height, cache.utxos)]
}

pub fn prep_rollback(block: &BlockData, cache: &mut cache::Metrics) -> Vec<SQLStatement> {
    // let mut sql_statements: Vec<SQLStatement> = Vec::new();
    let height = block.height;

    // Old value is cached value minus diff
    cache.utxos -= block_utxo_diff(block);

    vec![utxos::delete_snapshot(height)]
}

pub fn prep_bootstrap(height: i32) -> Vec<SQLStatement> {
    vec![utxos::append_snapshot_from_height(height)]
}

pub fn prep_genesis() -> Vec<SQLStatement> {
    vec![utxos::insert_genesis_snapshot()]
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

/// Change in number of UTxO's
///
/// Number of block ouputs - number of block inputs
fn block_utxo_diff(block: &BlockData) -> i64 {
    block
        .transactions
        .iter()
        .map(|tx| tx.outputs.len())
        .sum::<usize>() as i64
        - block
            .transactions
            .iter()
            .map(|tx| tx.input_box_ids.len())
            .sum::<usize>() as i64
}

#[cfg(test)]
mod tests {
    use super::cache;
    use crate::db::SQLArg;
    use crate::parsing::testing::block_600k;
    use pretty_assertions::assert_eq;

    #[test]
    fn check_prep() -> () {
        let mut cache = cache::Metrics { utxos: 100 };
        let statements = super::prep(&block_600k(), &mut cache);
        assert_eq!(statements.len(), 1);

        // UTxO count - block 600k has 4 inputs and 6 outputs
        assert_eq!(statements[0].sql, super::utxos::INSERT_SNAPSHOT);
        assert_eq!(
            statements[0].args,
            vec![SQLArg::Integer(600000), SQLArg::BigInt(100 + 2),]
        );
        assert_eq!(cache.utxos, 100 + 2);
    }

    #[test]
    fn check_rollback() -> () {
        let mut cache = cache::Metrics { utxos: 100 };
        let statements = super::prep_rollback(&block_600k(), &mut cache);
        assert_eq!(statements.len(), 1);

        // UTxO count - block 600k has 4 inputs and 6 outputs
        assert_eq!(statements[0].sql, super::utxos::DELETE_SNAPSHOT);
        assert_eq!(statements[0].args, vec![SQLArg::Integer(600000),]);
        assert_eq!(cache.utxos, 98);
    }

    #[test]
    fn check_bootstrap() -> () {
        let statements = super::prep_bootstrap(600000);
        assert_eq!(statements.len(), 1);

        // UTxO count
        assert_eq!(statements[0].sql, super::utxos::APPEND_SNAPSHOT_FROM_HEIGHT);
        assert_eq!(statements[0].args[0], SQLArg::Integer(600000));
    }

    #[test]
    fn check_genesis() -> () {
        let statements = super::prep_genesis();
        assert_eq!(statements.len(), 1);

        // UTxO count
        assert_eq!(statements[0].sql, super::utxos::INSERT_GENESIS_SNAPSHOT);
        assert_eq!(statements[0].args, &[]);
    }
}

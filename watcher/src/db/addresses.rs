//! # Address properties
//!
//! Process blocks into address properties tables data.

pub(super) mod erg;
pub(super) mod erg_diffs;
pub(super) mod tokens;
pub(super) mod tokens_diffs;

use crate::parsing::BlockData;
use log::info;
use postgres::types::Type;
use postgres::Client;
use postgres::Transaction;
use std::time::Instant;

pub(super) fn include_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<()> {
    erg_diffs::include(tx, block);
    erg::include(tx, block);
    tokens_diffs::include(tx, block);
    tokens::include(tx, block);
    Ok(())
}

pub(super) fn rollback_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<()> {
    tokens::rollback(tx, block);
    tokens_diffs::rollback(tx, block);
    erg::rollback(tx, block);
    erg_diffs::rollback(tx, block);
    Ok(())
}

pub(super) fn bootstrap(client: &mut Client) -> anyhow::Result<()> {
    if is_bootstrapped(client) {
        return Ok(());
    }
    info!("Bootstrapping address properties");

    if !constraints_are_set(client) {
        // Bootstrapping relies on indexes, so constraints are set now.
        let mut tx = client.transaction()?;
        set_constraints(&mut tx);
        tx.commit()?;
    }

    // Retrieve height and timestamps to process
    let sync_height = match get_sync_height(client) {
        Some(h) => h,
        None => -1,
    };
    let blocks: Vec<(i32, i64)> = client
        .query(
            "
            select height, timestamp
            from core.headers
            where height > $1;",
            &[&sync_height],
        )
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect();

    // Bootstrapping will be performed in batches of 1000
    let batch_size = 1000;
    let batches = blocks.chunks(batch_size);
    let nb_batches = batches.len();

    for (ibatch, batch_blocks) in batches.enumerate() {
        let timer = Instant::now();
        let mut tx = client.transaction()?;

        // Prepare statements
        let stmt_erg_diffs_insert =
            tx.prepare_typed(erg_diffs::INSERT_DIFFS_FOR_HEIGHT, &[Type::INT4])?;
        let stmt_erg_update = tx.prepare_typed(erg::UPDATE_BALANCES, &[Type::INT4, Type::INT8])?;
        let stmt_erg_insert = tx.prepare_typed(erg::INSERT_BALANCES, &[Type::INT4, Type::INT8])?;
        let stmt_erg_delete = tx.prepare_typed(erg::DELETE_ZERO_BALANCES, &[])?;
        let stmt_tokens_diffs_insert =
            tx.prepare_typed(tokens_diffs::INSERT_DIFFS_FOR_HEIGHT, &[Type::INT4])?;
        let stmt_tokens_update = tx.prepare_typed(tokens::UPDATE_BALANCES, &[Type::INT4])?;
        let stmt_tokens_insert = tx.prepare_typed(tokens::INSERT_BALANCES, &[Type::INT4])?;
        let stmt_tokens_delete = tx.prepare_typed(tokens::DELETE_ZERO_BALANCES, &[])?;

        for (height, timestamp) in batch_blocks {
            // Diffs go first
            tx.execute(&stmt_erg_diffs_insert, &[&height]).unwrap();
            // then balances
            tx.execute(&stmt_erg_update, &[&height, &timestamp])
                .unwrap();
            tx.execute(&stmt_erg_insert, &[&height, &timestamp])
                .unwrap();
            tx.execute(&stmt_erg_delete, &[]).unwrap();
            // Same for tokens, diffs first
            tx.execute(&stmt_tokens_diffs_insert, &[&height]).unwrap();
            // then balances
            tx.execute(&stmt_tokens_update, &[&height]).unwrap();
            tx.execute(&stmt_tokens_insert, &[&height]).unwrap();
            tx.execute(&stmt_tokens_delete, &[]).unwrap();
        }

        tx.commit()?;

        info!(
            "Bootstrapping balances - batch {} / {} (processed in {}s)",
            ibatch + 1,
            nb_batches,
            timer.elapsed().as_secs()
        );
    }

    client
        .execute("update adr._log set bootstrapped = TRUE;", &[])
        .unwrap();

    Ok(())
}

fn is_bootstrapped(client: &mut Client) -> bool {
    let row = client
        .query_one("select bootstrapped from adr._log;", &[])
        .unwrap();
    row.get(0)
}

fn constraints_are_set(client: &mut Client) -> bool {
    let row = client
        .query_one("select constraints_set from adr._log;", &[])
        .unwrap();
    row.get(0)
}

/// Get sync height of balance tables.
fn get_sync_height(client: &mut Client) -> Option<i32> {
    // All tables are progressed in sync, so enough to probe only one.
    let row = client
        .query_one("select max(height) from adr.erg_diffs;", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(tx: &mut Transaction) {
    erg::set_constraints(tx);
    erg_diffs::set_constraints(tx);
    tokens::set_constraints(tx);
    tokens_diffs::set_constraints(tx);
    tx.execute("update adr._log set constraints_set = TRUE;", &[])
        .unwrap();
}

pub(super) mod replay {
    use super::erg;
    use postgres::Transaction;

    /// Create an instance of the balance tables as they were was at `height`.
    pub fn prepare(tx: &mut Transaction, height: i32, replay_id: &str) {
        // schema should never exist at this stage
        tx.execute(&format!("create schema {replay_id}_adr;"), &[])
            .unwrap();
        erg::replay::prepare(tx, height, replay_id);
    }

    /// Advance state to next `height`.
    ///
    /// Assumes current state is at `height` - 1.
    pub fn step(tx: &mut Transaction, height: i32, replay_id: &str) {
        erg::replay::step(tx, height, replay_id);
    }

    pub fn cleanup(tx: &mut Transaction, id: &str) {
        tx.execute(&format!("drop schema if exists {id}_adr cascade;"), &[])
            .unwrap();
    }
}

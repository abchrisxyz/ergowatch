//! # Address properties
//!
//! Process blocks into address properties tables data.

pub(super) mod erg;
pub(super) mod erg_diffs;
pub(super) mod tokens;
pub(super) mod tokens_diffs;

use super::metrics::supply_age::SupplyAgeDiffs as SAD;
use crate::parsing::BlockData;
use log::info;
use postgres::types::Type;
use postgres::Client;
use postgres::Transaction;
use std::time::Instant;

pub(super) fn include_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<SAD> {
    erg_diffs::include(tx, block);
    let supply_age_diffs = erg::include(tx, block);
    tokens_diffs::include(tx, block);
    tokens::include(tx, block);
    Ok(supply_age_diffs)
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
    use crate::db::metrics::supply_age::SupplyAgeDiffs;
    use postgres::Transaction;

    /// Create an instance of this schema as it was at `height`.
    ///
    /// Will panic if a schema already exists for given `replay_id`.
    pub fn prepare(tx: &mut Transaction, height: i32, replay_id: &str) {
        let schema_name = format!("{replay_id}_adr");

        // schema should never exist at this stage
        tx.execute(&format!("create schema {schema_name};"), &[])
            .unwrap();
        erg::replay::prepare(tx, height, replay_id);
    }

    /// Create an instance of this schema as it was at `height`.
    ///
    /// Will panic if a schema already exists for given `replay_id`.
    pub fn prepare_with_age(tx: &mut Transaction, height: i32, replay_id: &str) {
        assert!(height < 0);
        let schema_name = format!("{replay_id}_adr");

        // schema should never exist at this stage
        tx.execute(&format!("create schema {schema_name};"), &[])
            .unwrap();
        erg::replay::create_with_age(tx, replay_id);
    }

    /// Create an instance of this schema as it was at `height`.
    ///
    /// Will panic if a schema already exists for given `replay_id` unless it's
    /// height matches the target `height`. This is useful
    /// to resume interupted bootstrap sessions.
    pub fn prepare_or_resume(tx: &mut Transaction, height: i32, replay_id: &str) {
        let schema_name = format!("{replay_id}_adr");

        // Check for existing instance
        let schema_exists: bool = tx
            .query_one(
                "
                select exists(
                    select schema_name
                    from information_schema.schemata
                    where schema_name = $1
                );",
                &[&schema_name],
            )
            .unwrap()
            .get(0);

        if schema_exists {
            resume(tx, height, replay_id);
        } else {
            prepare(tx, height, replay_id);
        }
    }

    /// Checks schema exists for given `replay_id` and it is at given `height`.
    pub fn resume(tx: &mut Transaction, height: i32, replay_id: &str) {
        let schema_name = format!("{replay_id}_adr");

        // Check for existing instance
        let schema_exists: bool = tx
            .query_one(
                "
                select exists(
                    select schema_name
                    from information_schema.schemata
                    where schema_name = $1
                );",
                &[&schema_name],
            )
            .unwrap()
            .get(0);

        if !schema_exists {
            panic!("Tried to resume replay for {schema_name} but no existing instance found");
        }

        // Verify existing instance is indeed at `height` by
        // recalculating *_adr.erg (only balances, no age timestamps) and comparing.
        // Number of records and balance for each address should match.
        let check_id = format!("{replay_id}_check");
        tx.execute(&format!("create schema {check_id}_adr;"), &[])
            .unwrap();
        erg::replay::prepare(tx, height, &check_id);

        let check: bool = tx
            .query_one(
                &format!(
                    "
                    select count(*) = 0 or bool_and(c.value is not null and c.value = b.value)
                    from {schema_name}.erg b
                    left join {check_id}_adr.erg c on c.address_id = b.address_id;"
                ),
                &[],
            )
            .unwrap()
            .get(0);

        tx.execute(&format!("drop schema {check_id}_adr cascade;"), &[])
            .unwrap();

        if !check {
            panic!("Found existing instance of {schema_name} but height doesn't match")
        }
    }

    /// Advance state to next `height`.
    ///
    /// Assumes current state is at `height` - 1.
    pub fn step(tx: &mut Transaction, height: i32, replay_id: &str) {
        erg::replay::step(tx, height, replay_id);
    }

    /// Advance state to next `height`.
    ///
    /// Assumes current state is at `height` - 1.
    pub fn step_with_age(
        tx: &mut Transaction,
        height: i32,
        timestamp: i64,
        replay_id: &str,
    ) -> SupplyAgeDiffs {
        erg::replay::step_with_age(tx, height, timestamp, replay_id)
    }

    pub fn cleanup(tx: &mut Transaction, id: &str) {
        tx.execute(&format!("drop schema if exists {id}_adr cascade;"), &[])
            .unwrap();
    }
}

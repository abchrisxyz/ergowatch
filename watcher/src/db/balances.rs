//! # balances
//!
//! Process blocks into balance tables data.

pub(super) mod erg;
pub(super) mod erg_diffs;
pub(super) mod tokens;
pub(super) mod tokens_diffs;

use crate::parsing::BlockData;
use log::info;
use postgres::types::Type;
use postgres::Transaction;

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

pub(super) fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    info!("Bootstrapping balances");

    if is_bootstrapped(tx) {
        info!("Already bootstrapped");
        return Ok(());
    }

    tx.execute("set local work_mem = '32MB';", &[]).unwrap();

    let row = tx.query_one(
        "
        select min(height) as min_height
            , max(height) as max_height
        from core.headers;",
        &[],
    )?;
    let first_height: i32 = row.get("min_height");
    let sync_height: i32 = row.get("max_height");

    // Bootstrapping queries rely on indexes, so constraints are set now.
    set_constraints(tx);

    // Prepare statements
    let stmt_erg_diffs_insert =
        tx.prepare_typed(erg_diffs::INSERT_DIFFS_FOR_HEIGHT, &[Type::INT4])?;
    let stmt_erg_update = tx.prepare_typed(erg::UPDATE_BALANCES, &[Type::INT4])?;
    let stmt_erg_insert = tx.prepare_typed(erg::INSERT_BALANCES, &[Type::INT4])?;
    let stmt_erg_delete = tx.prepare_typed(erg::DELETE_ZERO_BALANCES, &[])?;
    let stmt_tokens_diffs_insert =
        tx.prepare_typed(tokens_diffs::INSERT_DIFFS_FOR_HEIGHT, &[Type::INT4])?;
    let stmt_tokens_update = tx.prepare_typed(tokens::UPDATE_BALANCES, &[Type::INT4])?;
    let stmt_tokens_insert = tx.prepare_typed(tokens::INSERT_BALANCES, &[Type::INT4])?;
    let stmt_tokens_delete = tx.prepare_typed(tokens::DELETE_ZERO_BALANCES, &[])?;

    set_tables_unlogged(tx);

    for h in first_height..sync_height + 1 {
        // Diffs go first
        tx.execute(&stmt_erg_diffs_insert, &[&h]).unwrap();
        // then balances
        tx.execute(&stmt_erg_update, &[&h]).unwrap();
        tx.execute(&stmt_erg_insert, &[&h]).unwrap();
        tx.execute(&stmt_erg_delete, &[]).unwrap();

        // Same for tokens, diffs first
        tx.execute(&stmt_tokens_diffs_insert, &[&h]).unwrap();
        // then balances
        tx.execute(&stmt_tokens_update, &[&h]).unwrap();
        tx.execute(&stmt_tokens_insert, &[&h]).unwrap();
        tx.execute(&stmt_tokens_delete, &[]).unwrap();
    }

    set_tables_logged(tx);

    Ok(())
}

fn is_bootstrapped(tx: &mut Transaction) -> bool {
    // If tables are not empty, tables are bootstrapped already.
    // All tables are progressed in sync, so enough to check only one.
    let row = tx
        .query_one("select exists(select * from bal.erg limit 1);", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(tx: &mut Transaction) {
    tx.execute(erg::constraints::ADD_PK, &[]).unwrap();
    tx.execute(erg::constraints::CHECK_VALUE_GE0, &[]).unwrap();
    tx.execute(erg::constraints::IDX_VALUE, &[]).unwrap();
    tx.execute(erg_diffs::constraints::ADD_PK, &[]).unwrap();
    tx.execute(erg_diffs::constraints::IDX_HEIGHT, &[]).unwrap();
    tx.execute(tokens::constraints::ADD_PK, &[]).unwrap();
    tx.execute(tokens::constraints::CHECK_VALUE_GE0, &[])
        .unwrap();
    tx.execute(tokens::constraints::IDX_VALUE, &[]).unwrap();
    tx.execute(tokens_diffs::constraints::ADD_PK, &[]).unwrap();
    tx.execute(tokens_diffs::constraints::IDX_HEIGHT, &[])
        .unwrap();
}

fn set_tables_logged(tx: &mut Transaction) {
    tx.execute("alter table bal.erg set logged;", &[]).unwrap();
    tx.execute("alter table bal.erg_diffs set logged;", &[])
        .unwrap();
    tx.execute("alter table bal.tokens set logged;", &[])
        .unwrap();
    tx.execute("alter table bal.tokens_diffs set logged;", &[])
        .unwrap();
}

fn set_tables_unlogged(tx: &mut Transaction) {
    tx.execute("alter table bal.erg set unlogged;", &[])
        .unwrap();
    tx.execute("alter table bal.erg_diffs set unlogged;", &[])
        .unwrap();
    tx.execute("alter table bal.tokens set unlogged;", &[])
        .unwrap();
    tx.execute("alter table bal.tokens_diffs set unlogged;", &[])
        .unwrap();
}

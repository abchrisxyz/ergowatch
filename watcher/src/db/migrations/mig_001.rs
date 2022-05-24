/// Migration 1
///
/// Adds mtr schema and mtr.utxos table.
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("create schema mtr;", &[])?;
    tx.execute("create table mtr.utxos(height int, value bigint);", &[])?;
    Ok(())
}

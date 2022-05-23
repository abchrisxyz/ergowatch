/// Migration 3
///
/// Drop constraints table
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("drop table ew.constraints cascade;", &[])?;
    Ok(())
}

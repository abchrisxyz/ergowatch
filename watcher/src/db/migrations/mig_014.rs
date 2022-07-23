/// Migration 14
///
/// Add BRIN index on headers timestamp
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create index on core.headers using brin(timestamp);",
        &[],
    )?;

    Ok(())
}

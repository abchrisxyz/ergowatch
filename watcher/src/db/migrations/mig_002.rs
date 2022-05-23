/// Migration 2
///
/// Fixes genesis box timestamps
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "update core.headers set timestamp = 1561978800000 where height = 0",
        &[],
    )?;
    Ok(())
}

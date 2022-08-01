/// Migration 15
///
/// Rename bal schema
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("alter schema bal rename to adr;", &[])?;
    Ok(())
}

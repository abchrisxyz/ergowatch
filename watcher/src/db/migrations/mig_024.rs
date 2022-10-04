/// Migration 24
///
/// Enforce address id's > 0
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("alter table core.addresses add check (id > 0);", &[])?;
    Ok(())
}

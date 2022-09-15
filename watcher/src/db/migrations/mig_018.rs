/// Migration 18
///
/// Add miner flag to core.addresses
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("alter table core.addresses add column miner boolean;", &[])?;
    tx.execute("update core.addresses set miner = false;", &[])?;
    tx.execute(
        "
        update core.addresses
        set miner = true
        where address like '88dhgzEuTX%';",
        &[],
    )?;
    tx.execute(
        "alter table core.addresses alter column miner set not null;",
        &[],
    )?;
    Ok(())
}

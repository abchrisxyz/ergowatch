/// Migration 16
///
/// Add p2pk flag to core.addresses
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("alter table core.addresses add column p2pk boolean;", &[])?;
    tx.execute("update core.addresses set p2pk = false;", &[])?;
    tx.execute(
        "
        update core.addresses
        set p2pk = true
        where address like '9%' and length(address) = 51;",
        &[],
    )?;
    tx.execute(
        "alter table core.addresses alter column p2pk set not null;",
        &[],
    )?;
    Ok(())
}

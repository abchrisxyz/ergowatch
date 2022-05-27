/// Migration 9
///
/// Add cex supply metrics
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.cex_supply (
            height int,
            total bigint,
            deposit bigint
        );",
        &[],
    )?;

    Ok(())
}

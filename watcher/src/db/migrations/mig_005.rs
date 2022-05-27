/// Migration 5
///
/// Add cex supply table
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "create table cex.supply (
            height int,
            cex_id integer,
            main bigint,
            deposit bigint
        );",
        &[],
    )?;
    Ok(())
}

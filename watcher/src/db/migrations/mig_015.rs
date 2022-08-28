/// Migration 15
///
/// Rename bal schema add adr._log table and erg mean age timestamp
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("alter schema bal rename to adr;", &[])?;
    tx.execute(
        "alter table adr.erg add column mean_age_timestamp bigint;",
        &[],
    )?;
    // Truncate adr tables and set flag to trigger bootstrapping.
    tx.execute(
        "truncate table adr.erg, adr.erg_diffs, adr.tokens, adr.tokens_diffs;",
        &[],
    )?;
    tx.execute("update adr._log set bootstrapped = FALSE;", &[])?;
    // Constraints can be left as they are since adr bootstrapping relies on them.
    // Just adding new one here.
    tx.execute(
        "alter table adr.erg alter column mean_age_timestamp set not null;",
        &[],
    )?;
    Ok(())
}

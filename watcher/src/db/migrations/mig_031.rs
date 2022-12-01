/// Migration 31
///
/// Fix early supply age records with unhandled zero timestamps
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    // Truncate supply age tables set flag to trigger bootstrapping.
    tx.execute(
        "truncate table mtr.supply_age_timestamps, mtr.supply_age_days;",
        &[],
    )?;
    tx.execute("update mtr._log set supply_age_bootstrapped = FALSE;", &[])?;

    Ok(())
}

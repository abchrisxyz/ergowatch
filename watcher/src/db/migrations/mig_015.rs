/// Migration 15
///
/// Rename bal schema
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("alter schema bal rename to adr;", &[])?;
    tx.execute(
        "alter table adr.erg add column mean_age_timestamp bigint;",
        &[],
    )?;
    // TODO: calc timestamps
    // tx.execute(
    //     "alter table adr.erg alter column mean_age_timestamp set not null;",
    //     &[],
    // )?;
    Ok(())
}

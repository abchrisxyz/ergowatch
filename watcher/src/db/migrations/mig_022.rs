/// Migration 22
///
/// Add volume metrics
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.volume(
            height int,
            daily_1d bigint,
            daily_7d bigint,
            daily_28d bigint
        );",
        &[],
    )?;
    tx.execute(
        "alter table mtr._log add column volume_constraints_set bool not null default FALSE;",
        &[],
    )?;
    Ok(())
}

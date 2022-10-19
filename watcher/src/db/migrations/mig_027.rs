/// Migration 27
///
/// Add metrics periodic timestamp heights
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.timestamps_daily (
            height int,
            timestamp bigint
        );",
        &[],
    )?;
    tx.execute(
        "
        create table mtr.timestamps_hourly (
            height int,
            timestamp bigint
        );",
        &[],
    )?;
    tx.execute(
        "
        alter table mtr._log add column timestamps_constraints_set bool not null default FALSE
        ;",
        &[],
    )?;
    Ok(())
}

/// Migration 23
///
/// Add supply age metrics
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.supply_age_timestamps (
            height int,
            overall bigint,
            p2pks bigint,
            cexs bigint,
            contracts bigint,
            miners bigint
        );",
        &[],
    )?;
    tx.execute(
        "
        create table mtr.supply_age_days (
            height int,
            overall real,
            p2pks real,
            cexs real,
            contracts real,
            miners real
        );",
        &[],
    )?;
    tx.execute(
        "alter table mtr._log
            add column supply_age_bootstrapped bool not null default FALSE,
            add column supply_age_constraints_set bool not null default FALSE;
        ",
        &[],
    )?;
    Ok(())
}

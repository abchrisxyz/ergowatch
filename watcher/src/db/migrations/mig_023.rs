/// Migration 23
///
/// Add supply age metrics
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.supply_age (
            height int,
            secs_all bigint,
            secs_p2pk bigint,
            secs_exchanges bigint,
            secs_contracts bigint,
            secs_miners bigint
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

/// Migration 13
///
/// Add erg/usd data for metrics
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.ergusd (
            height int,
            value double precision
        );",
        &[],
    )?;

    tx.execute(
        "
        create table mtr.ergusd_provisional (
            height int
        );",
        &[],
    )?;

    tx.execute(
        "
        create table mtr._log (
            singleton int primary key default 1,
            ergusd_constraints_set bool not null default FALSE,
            ergusd_bootstrapped bool not null default FALSE,
            check(singleton = 1)
        );",
        &[],
    )?;

    tx.execute(
        "
        insert into mtr._log(singleton) values (1);",
        &[],
    )?;

    Ok(())
}

/// Migration 20
///
/// Add block stats
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("create schema blk;", &[])?;
    tx.execute(
        "create table blk._log (
            singleton int primary key default 1,
            constraints_set bool not null default FALSE,
            bootstrapped bool not null default FALSE,
            check(singleton = 1)
        );",
        &[],
    )?;
    tx.execute("insert into blk._log(singleton) values (1);", &[])?;
    tx.execute(
        "create table blk.stats (
            height int,
            circulating_supply bigint,
            emission bigint,
            reward bigint,
            tx_fees bigint,
            tx_count bigint,
            volume bigint
        );",
        &[],
    )?;
    Ok(())
}

/// Migration 17
///
/// Add address counts metrics
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.address_counts_by_balance_p2pk (
            height int,
            total bigint,
            ge_0p001 bigint,
            ge_0p01 bigint,
            ge_0p1 bigint,
            ge_1 bigint,
            ge_10 bigint,
            ge_100 bigint,
            ge_1k bigint,
            ge_10k bigint,
            ge_100k bigint,
            ge_1m bigint
        );",
        &[],
    )?;
    tx.execute(
        "
        create table mtr.address_counts_by_balance_contracts (
            height int,
            total bigint,
            ge_0p001 bigint,
            ge_0p01 bigint,
            ge_0p1 bigint,
            ge_1 bigint,
            ge_10 bigint,
            ge_100 bigint,
            ge_1k bigint,
            ge_10k bigint,
            ge_100k bigint,
            ge_1m bigint
        );",
        &[],
    )?;
    tx.execute(
        "alter table mtr._log
            add column address_counts_constraints_set bool not null default FALSE,
            add column address_counts_bootstrapped bool not null default FALSE
            ",
        &[],
    )?;
    Ok(())
}

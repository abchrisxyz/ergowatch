/// Migration 19
///
/// Add supply distribution metrics
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.supply_on_top_addresses_p2pk (
            height int,
            top_1_prc bigint,
            top_1k bigint,
            top_100 bigint,
            top_10 bigint
        );",
        &[],
    )?;
    tx.execute(
        "
        create table mtr.supply_on_top_addresses_contracts (
            height int,
            top_1_prc bigint,
            top_1k bigint,
            top_100 bigint,
            top_10 bigint
        );",
        &[],
    )?;
    tx.execute(
        "
        create table mtr.supply_on_top_addresses_miners (
            height int,
            top_1_prc bigint,
            top_1k bigint,
            top_100 bigint,
            top_10 bigint
        );",
        &[],
    )?;
    tx.execute(
        "alter table mtr._log
            add column supply_distribution_constraints_set bool not null default FALSE,
            add column supply_distribution_bootstrapped bool not null default FALSE
            ",
        &[],
    )?;
    Ok(())
}
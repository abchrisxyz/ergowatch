/// Migration 25
///
/// Add supply composition to metrics
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.supply_composition (
            height int,
            p2pks bigint,
            cex_main bigint,
            cex_deposits bigint,
            contracts bigint,
            miners bigint,
            treasury bigint
        );",
        &[],
    )?;
    tx.execute(
        "
        alter table mtr._log
            add column supply_composition_bootstrapped bool not null default FALSE,
            add column supply_composition_constraints_set bool not null default FALSE
        ;",
        &[],
    )?;
    Ok(())
}

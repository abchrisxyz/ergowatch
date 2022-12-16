use std::str::FromStr;

/// Migration 32
///
/// Simplify repair process
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    // Drop deprecated repair infra
    tx.execute("drop table ew.repairs;", &[])?;

    // Drop deprecated cex tables
    tx.execute("drop table cex.block_processing_log;", &[])?;
    tx.execute("drop type cex.t_block_status;", &[])?;
    tx.execute("drop table cex.addresses;", &[])?;
    tx.execute("drop table cex.addresses_conflicts;", &[])?;
    tx.execute("drop type cex.t_address_type;", &[])?;

    // Split cex addesses between main and deposits
    tx.execute(
        "
        create table cex.main_addresses (
            address_id bigint,
            cex_id integer,
            listing_height integer
        );
        ",
        &[],
    )?;
    tx.execute(
        "
        create table cex.deposit_addresses (
            address_id bigint,
            cex_id integer,
            spot_height int
        );",
        &[],
    )?;
    tx.execute(
        "
        create table cex.deposit_addresses_excluded (
            address_id bigint,
            address_spot_height integer,
            conflict_spot_height integer
        );",
        &[],
    )?;

    // Rename ignored deposits
    tx.execute(
        "
        alter table cex.addresses_ignored
            rename to deposit_addresses_ignored;",
        &[],
    )?;
    // and truncate as bootstrapping will repopulate
    tx.execute("truncate table cex.deposit_addresses_ignored;", &[])?;

    // New cex log table
    tx.execute(
        "
        create table cex._deposit_addresses_log (
            singleton int primary key default 1,
            last_processed_height int default 0,
            check(singleton = 1)
        );",
        &[],
    )?;
    tx.execute(
        "
        insert into cex._deposit_addresses_log(singleton) values (1);",
        &[],
    )?;

    // Drop some constraints on truncated tables because bootstrapping will reapply them
    drop_all_constraints(tx, "mtr", "supply_composition");
    drop_all_constraints(tx, "mtr", "supply_age_timestamps");
    drop_all_constraints(tx, "mtr", "supply_age_days");
    drop_all_constraints(tx, "mtr", "cex_supply");
    drop_all_constraints(tx, "cex", "deposit_addresses_ignored");
    drop_all_constraints(tx, "cex", "supply");
    drop_all_constraints(tx, "cex", "cexs");

    // Supply history will be repopulated by bootstrapping.
    tx.execute("truncate table cex.supply;", &[])?;

    // Truncate cex supply metrics table to trigger bootstrapping.
    tx.execute("truncate table mtr.cex_supply;", &[])?;

    // Truncate supply composition tables set flag to trigger bootstrapping.
    tx.execute("truncate table mtr.supply_composition;", &[])?;
    tx.execute(
        "update mtr._log set supply_composition_bootstrapped = FALSE;",
        &[],
    )?;
    tx.execute(
        "update mtr._log set supply_composition_constraints_set = FALSE;",
        &[],
    )?;

    // Truncate supply age tables set flag to trigger bootstrapping.
    tx.execute("truncate table mtr.supply_age_timestamps;", &[])?;
    tx.execute("truncate table mtr.supply_age_days;", &[])?;
    tx.execute("update mtr._log set supply_age_bootstrapped = FALSE;", &[])?;
    tx.execute(
        "update mtr._log set supply_age_constraints_set = FALSE;",
        &[],
    )?;

    Ok(())
}

fn drop_all_constraints(tx: &mut Transaction, schema: &str, table: &str) {
    let constraints: Vec<String> = tx
        .query(
            "
            select con.conname
            from pg_catalog.pg_constraint con
            join pg_catalog.pg_class rel
                on rel.oid = con.conrelid
            join pg_catalog.pg_namespace nsp
                on nsp.oid = connamespace
            where nsp.nspname = $1
                and rel.relname = $2;
            ",
            &[&schema, &table],
        )
        .unwrap()
        .iter()
        .map(|r| String::from_str(r.get(0)).unwrap())
        .collect();

    for constraint in &constraints {
        tx.execute(
            &format!("alter table {schema}.{table} drop constraint {constraint};"),
            &[],
        )
        .unwrap();
    }
}

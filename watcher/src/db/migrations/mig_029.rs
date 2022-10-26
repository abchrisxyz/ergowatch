/// Migration 29
///
/// Add metrics summary tables
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    let bigint_tables = vec![
        "utxos_summary",
        "address_counts_by_balance_p2pk_summary",
        "address_counts_by_balance_contracts_summary",
        "address_counts_by_balance_miners_summary",
        "supply_composition_summary",
        "supply_on_top_addresses_p2pk_summary",
        "supply_on_top_addresses_contracts_summary",
        "supply_on_top_addresses_miners_summary",
    ];

    for table in bigint_tables {
        tx.execute(
            &format!(
                "
                create table mtr.{table} (
                    label text not null primary key,
                    current bigint not null,
                    diff_1d bigint not null,
                    diff_1w bigint not null,
                    diff_4w bigint not null,
                    diff_6m bigint not null,
                    diff_1y bigint not null
                );"
            ),
            &[],
        )?;
    }

    // Real tables
    tx.execute(
        &format!(
            "
            create table mtr.supply_age_days_summary (
                label text not null primary key,
                current real not null,
                diff_1d real not null,
                diff_1w real not null,
                diff_4w real not null,
                diff_6m real not null,
                diff_1y real not null
            );"
        ),
        &[],
    )?;
    Ok(())
}

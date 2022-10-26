/// Migration 29
///
/// Add metrics summary tables
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    let tables = vec![
        "utxos_summary",
        "address_counts_by_balance_p2pk_summary",
        "address_counts_by_balance_contracts_summary",
        "address_counts_by_balance_miners_summary",
        "supply_composition_summary",
    ];

    for table in tables {
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
    Ok(())
}

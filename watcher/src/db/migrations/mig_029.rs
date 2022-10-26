/// Migration 29
///
/// Add metrics summary tables
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        create table mtr.utxos_summary (
            label text not null primary key,
            current bigint not null,
            diff_1d bigint not null,
            diff_1w bigint not null,
            diff_4w bigint not null,
            diff_6m bigint not null,
            diff_1y bigint not null
        );",
        &[],
    )?;
    tx.execute(
        "
        create table mtr.address_counts_by_balance_p2pk_summary (
            label text not null primary key,
            current bigint not null,
            diff_1d bigint not null,
            diff_1w bigint not null,
            diff_4w bigint not null,
            diff_6m bigint not null,
            diff_1y bigint not null
        );",
        &[],
    )?;
    tx.execute(
        "
        create table mtr.address_counts_by_balance_contracts_summary (
            label text not null primary key,
            current bigint not null,
            diff_1d bigint not null,
            diff_1w bigint not null,
            diff_4w bigint not null,
            diff_6m bigint not null,
            diff_1y bigint not null
        );",
        &[],
    )?;
    tx.execute(
        "
        create table mtr.address_counts_by_balance_miners_summary (
            label text not null primary key,
            current bigint not null,
            diff_1d bigint not null,
            diff_1w bigint not null,
            diff_4w bigint not null,
            diff_6m bigint not null,
            diff_1y bigint not null
        );",
        &[],
    )?;
    Ok(())
}

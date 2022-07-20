/// Migration 12
///
/// Add cgo schema for coingecko
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("create schema cgo;", &[])?;
    tx.execute(
        "
        create table cgo.ergusd (
            timestamp bigint primary key not null,
            value double precision not null
        );",
        &[],
    )?;

    Ok(())
}

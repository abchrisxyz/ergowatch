use postgres_from_row::FromRow;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::AddressCounts;
use super::super::types::AddressCountsRecord;
use crate::core::types::Height;

pub(super) async fn get_last(client: &Client) -> AddressCounts {
    AddressCounts {
        p2pk: get_last_p2pk(client).await,
        contracts: get_last_contracts(client).await,
        miners: get_last_miners(client).await,
    }
}

async fn get_last_p2pk(client: &Client) -> AddressCountsRecord {
    let sql = "
        select *
        from erg.address_counts_by_balance_p2pk
        order by height desc limit 1;
    ";
    match client.query_opt(sql, &[]).await.unwrap() {
        Some(row) => AddressCountsRecord::from_row(&row),
        None => AddressCountsRecord::blank(),
    }
}

async fn get_last_contracts(client: &Client) -> AddressCountsRecord {
    let sql = "
        select *
        from erg.address_counts_by_balance_contracts
        order by height desc limit 1;
    ";
    match client.query_opt(sql, &[]).await.unwrap() {
        Some(row) => AddressCountsRecord::from_row(&row),
        None => AddressCountsRecord::blank(),
    }
}

async fn get_last_miners(client: &Client) -> AddressCountsRecord {
    let sql = "
        select *
        from erg.address_counts_by_balance_miners
        order by height desc limit 1;
    ";
    match client.query_opt(sql, &[]).await.unwrap() {
        Some(row) => AddressCountsRecord::from_row(&row),
        None => AddressCountsRecord::blank(),
    }
}

pub(super) async fn insert(pgtx: &Transaction<'_>, counts: &AddressCounts) {
    insert_record(pgtx, &counts.p2pk, "p2pk").await;
    insert_record(pgtx, &counts.contracts, "contracts").await;
    insert_record(pgtx, &counts.miners, "miners").await;
}

/// Inserts a record into table matching given `label`.
async fn insert_record(pgtx: &Transaction<'_>, record: &AddressCountsRecord, label: &str) {
    let sql = format!(
        "
        insert into erg.address_counts_by_balance_{label} (
            height,
            total,
            ge_0p001,
            ge_0p01,
            ge_0p1,
            ge_1,
            ge_10,
            ge_100,
            ge_1k,
            ge_10k,
            ge_100k,
            ge_1m 
        ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, &12);
    "
    );
    pgtx.execute(
        &sql,
        &[
            &record.height,
            &record.total,
            &record.ge_0p001,
            &record.ge_0p01,
            &record.ge_0p1,
            &record.ge_1,
            &record.ge_10,
            &record.ge_100,
            &record.ge_1k,
            &record.ge_10k,
            &record.ge_100k,
            &record.ge_1m,
        ],
    )
    .await
    .unwrap();
}

/// Delete records for given `height`.
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    // P2PK's
    pgtx.execute(
        "delete from erg.address_counts_by_balance_p2pk where height = $1;",
        &[&height],
    )
    .await
    .unwrap();

    // Contracts
    pgtx.execute(
        "delete from erg.address_counts_by_balance_contracts where height = $1;",
        &[&height],
    )
    .await
    .unwrap();

    // Miners
    pgtx.execute(
        "delete from erg.address_counts_by_balance_miners where height = $1;",
        &[&height],
    )
    .await
    .unwrap();
}

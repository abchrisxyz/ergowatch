use postgres_from_row::FromRow;
use tokio_postgres::Client;

use super::super::types::AddressCounts;
use super::super::types::AddressCountsRecord;

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

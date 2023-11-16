use postgres_from_row::FromRow;
use std::collections::HashMap;
use tokio_postgres::types::Type;
use tokio_postgres::Client;
use tokio_postgres::GenericClient;
use tokio_postgres::Transaction;

use super::super::types::DepositAddressRecord;
use super::super::types::ExchangeID;
use crate::core::types::AddressID;
use crate::core::types::Height;

pub(super) async fn insert_many(pgtx: &Transaction<'_>, records: &Vec<DepositAddressRecord>) {
    tracing::trace!("insert_many {records:?}");
    if records.is_empty() {
        // Nothing to do, return.
        return;
    }
    let sql = "
        insert into exchanges.deposit_addresses (address_id, cex_id, spot_height)
        values ($1, $2, $3);
    ";
    let stmt = pgtx
        .prepare_typed(sql, &[Type::INT8, Type::INT4, Type::INT4])
        .await
        .unwrap();
    for rec in records {
        pgtx.execute(&stmt, &[&rec.address_id, &rec.cex_id, &rec.spot_height])
            .await
            .unwrap();
    }
}

pub(super) async fn map_all(client: &impl GenericClient) -> HashMap<AddressID, ExchangeID> {
    tracing::trace!("map_all");
    let qry = "
        select address_id
            , cex_id
        from exchanges.deposit_addresses;
    ";
    HashMap::from_iter(
        client
            .query(qry, &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.get(0), r.get(1))),
    )
}

pub(super) async fn get_one(pgtx: &Transaction<'_>, address_id: AddressID) -> DepositAddressRecord {
    tracing::trace!("get_one {address_id:?}");
    let qry = "
        select address_id
            , cex_id
            , spot_height
        from exchanges.deposit_addresses
        where address_id = $1;
    ";
    DepositAddressRecord::from_row(&pgtx.query_one(qry, &[&address_id]).await.unwrap())
}

pub(super) async fn get_spotted_at(client: &Client, height: Height) -> Vec<AddressID> {
    tracing::trace!("get_spotted_at {height}");
    let qry = "
        select address_id
        from exchanges.deposit_addresses
        where spot_height = $1;
    ";
    let rows = client.query(qry, &[&height]).await.unwrap();
    rows.iter().map(|r| r.get(0)).collect()
}

pub(super) async fn delete_spotted_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_spotted_at {height}");
    let sql = "
        delete from exchanges.deposit_addresses
        where spot_height = $1;
    ";
    pgtx.query(sql, &[&height]).await.unwrap();
}

pub(super) async fn delete_one(pgtx: &Transaction<'_>, address_id: AddressID) {
    tracing::trace!("delete_one {address_id:?}");
    pgtx.execute(
        "delete from exchanges.deposit_addresses where address_id = $1",
        &[&address_id],
    )
    .await
    .unwrap();
}

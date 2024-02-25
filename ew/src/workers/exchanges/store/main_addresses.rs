use std::collections::HashMap;
use tokio_postgres::GenericClient;

use super::super::types::ExchangeID;
use super::super::types::MainAddressRecord;
use crate::core::types::AddressID;

pub(super) async fn map_all(client: &impl GenericClient) -> HashMap<AddressID, ExchangeID> {
    tracing::trace!("map_all");
    let qry = "
        select address_id
            , cex_id
        from exchanges.main_addresses;
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

pub(super) async fn insert(client: &impl GenericClient, record: &MainAddressRecord) {
    tracing::trace!("insert {record:?}");
    let sql = "
        insert into exchanges.main_addresses (address_id, cex_id, address)
        values ($1, $2, $3);
    ";
    client
        .execute(sql, &[&record.address_id, &record.cex_id, &record.address])
        .await
        .unwrap();
}

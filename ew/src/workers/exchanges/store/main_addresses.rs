use std::collections::HashMap;
use tokio_postgres::GenericClient;

use super::super::types::ExchangeID;
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

use std::collections::HashSet;
use tokio_postgres::GenericClient;

use crate::core::types::AddressID;

pub(super) async fn get_all(client: &impl GenericClient) -> HashSet<AddressID> {
    tracing::trace!("get_all");
    let qry = "
        select address_id
        from exchanges.deposit_addresses_excluded;
    ";
    HashSet::from_iter(
        client
            .query(qry, &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| r.get(0)),
    )
}

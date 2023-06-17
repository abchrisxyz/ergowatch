use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::Address;
use crate::core::types::AddressID;
use crate::core::types::Height;

#[derive(Debug)]
pub struct AddressRecord {
    pub id: AddressID,
    pub spot_height: Height,
    pub address: Address,
}

impl AddressRecord {
    pub fn new(id: AddressID, spot_height: Height, address: Address) -> Self {
        Self {
            id,
            spot_height,
            address,
        }
    }
}

/// Retrieve id of an existing address.
pub(super) async fn get_id(pgtx: &Transaction<'_>, address: &Address) -> AddressID {
    let qry = "select core.address_id($1);";
    pgtx.query_one(qry, &[address]).await.unwrap().get(0)
}

/// Retrieve id of a possibly unknown address.
pub(super) async fn get_id_opt(pgtx: &Transaction<'_>, address: &Address) -> Option<AddressID> {
    let qry = "select core.address_id($1);";
    // core.address_id() will return null for an unknown address,
    // so there's always a row.
    pgtx.query_one(qry, &[address]).await.unwrap().get(0)
}

/// Retrieve highest address id.
pub(super) async fn get_max_id(client: &Client) -> AddressID {
    let qry = "select max(id) from core.addresses;";
    match client.query_one(qry, &[]).await.unwrap().get(0) {
        Some(id) => id,
        None => 0,
    }
}

/// Insert new address and get new id back.
pub(super) async fn index_new(pgtx: &Transaction<'_>, rec: &AddressRecord) {
    tracing::debug!("inserting address: {:?}", rec);
    let stmt = "insert into core.addresses(id, spot_height, address) values ($1, $2, $3)";
    pgtx.execute(stmt, &[&rec.id, &rec.spot_height, &rec.address])
        .await
        .unwrap();
}

/// Delete addresses spotted at `height`
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    pgtx.execute(
        "delete from core.addresses where spot_height = $1;",
        &[&height],
    )
    .await
    .unwrap();
}

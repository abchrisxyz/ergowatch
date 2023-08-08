use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::AssetID;
use crate::core::types::Height;
use crate::core::types::TokenID;

#[derive(Debug)]
pub struct TokenRecord {
    pub asset_id: AssetID,
    pub spot_height: Height,
    pub token_id: TokenID,
}

impl TokenRecord {
    pub fn new(asset_id: AssetID, spot_height: Height, token_id: TokenID) -> Self {
        Self {
            asset_id,
            spot_height,
            token_id,
        }
    }
}

/// Retrieve id of a possibly unknown token_id.
pub(super) async fn get_id_opt(pgtx: &Transaction<'_>, token_id: &TokenID) -> Option<AssetID> {
    let qry = "select asset_id from core.tokens where token_id = $1;";
    match pgtx.query_opt(qry, &[token_id]).await.unwrap() {
        Some(row) => Some(row.get(0)),
        None => None,
    }
}

/// Retrieve highest asset id.
pub(super) async fn get_max_id(client: &Client) -> AssetID {
    let qry = "select max(asset_id) from core.tokens;";
    match client.query_one(qry, &[]).await.unwrap().get(0) {
        Some(id) => id,
        None => 0,
    }
}

/// Insert new token and get new id back.
pub(super) async fn index_new(pgtx: &Transaction<'_>, rec: &TokenRecord) {
    tracing::debug!("inserting token: {:?}", rec);
    let stmt = "insert into core.tokens(asset_id, spot_height, token_id) values ($1, $2, $3)";
    pgtx.execute(stmt, &[&rec.asset_id, &rec.spot_height, &rec.token_id])
        .await
        .unwrap();
}

/// Delete tokens spotted at `height`
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    pgtx.execute(
        "delete from core.tokens where spot_height = $1;",
        &[&height],
    )
    .await
    .unwrap();
}

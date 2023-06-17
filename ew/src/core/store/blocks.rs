use tokio_postgres::Client;
use tokio_postgres::Transaction;
use tracing::debug;

use crate::core::types::Head;
use crate::core::types::Height;

/// Retrieve head from latest block.
pub(super) async fn last_head(client: &Client) -> Head {
    debug!("Reading last block head");
    let qry = "select max(height) from core.blocks";
    let max_h: Option<Height> = client.query_one(qry, &[]).await.unwrap().get(0);
    debug!("Max height in core.blocks is {:?}", max_h);
    if max_h.is_none() {
        return Head::initial();
    }
    let qry = "
        select block -> 'header' -> 'height'
            , block -> 'header' -> 'id'
        from core.blocks
        order by height desc
        limit 1;";
    let row = client.query_one(qry, &[]).await.unwrap();
    return Head::new(
        serde_json::from_value(row.get(0)).unwrap(),
        serde_json::from_value(row.get(1)).unwrap(),
    );
}

pub async fn insert(pgtx: &Transaction<'_>, height: Height, block: String) {
    let stmt = "insert into core.blocks (height, block) values ($1, $2);";
    let jsonvalue: serde_json::Value = serde_json::from_str(&block).unwrap();
    pgtx.execute(stmt, &[&height, &jsonvalue]).await.unwrap();
}

/// Delete block at `height`
pub async fn delete(pgtx: &Transaction<'_>, height: Height) {
    pgtx.execute("delete from core.blocks where height = $1;", &[&height])
        .await
        .unwrap();
}

use tokio_postgres::Client;
use tokio_postgres::Transaction;
use tracing::debug;

use crate::core::types::Head;
use crate::core::types::Height;
use crate::core::types::Timestamp;

/// Retrieve head from latest header.
pub(super) async fn last_head(client: &Client) -> Head {
    debug!("reading last header");
    let qry = "select max(height) from core.headers";
    let max_h: Option<Height> = client.query_one(qry, &[]).await.unwrap().get(0);
    debug!("Max height in core.headers is {:?}", max_h);
    if max_h.is_none() {
        return Head::initial();
    }
    let qry = "
        select height
            , id
        from core.headers
        order by height desc
        limit 1;";
    let row = client.query_one(qry, &[]).await.unwrap();
    return Head::new(row.get(0), row.get(1));
}

pub async fn insert(pgtx: &Transaction<'_>, height: Height, timestamp: Timestamp, id: &String) {
    let stmt = "insert into core.headers (height, timestamp, id) values ($1, $2, $3);";
    pgtx.execute(stmt, &[&height, &timestamp, &id])
        .await
        .unwrap();
}

/// Delete block at `height`
pub async fn delete(pgtx: &Transaction<'_>, height: Height) {
    pgtx.execute("delete from core.headers where height = $1;", &[&height])
        .await
        .unwrap();
}

use tokio_postgres::Client;
use tokio_postgres::Transaction;
use tracing::trace;

use crate::core::types::Head;
use crate::core::types::Height;
use crate::core::types::Timestamp;

/// Retrieve head from latest header.
pub(super) async fn last_head(client: &Client) -> Head {
    trace!("reading last header");
    let qry = "select max(height) from core.headers";
    let max_h: Option<Height> = client.query_one(qry, &[]).await.unwrap().get(0);
    trace!("max height in core.headers is {:?}", max_h);
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

/// Returns `true` if core.headers has a record for given `head`.
pub async fn exists(client: &Client, head: &Head) -> bool {
    let sql = "
    select exists (
        select height
            , id
        from core.headers
        where height = $1 and id = $2
    );";
    client
        .query_one(sql, &[&head.height, &head.header_id])
        .await
        .unwrap()
        .get(0)
}

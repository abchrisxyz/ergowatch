use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::MiniHeader;
use crate::core::types::Height;

pub async fn get_last(client: &Client) -> MiniHeader {
    let qry = "
        select height
            , timestamp
            , id
        from sigmausd.headers
        order by height desc
        limit 1;";
    let row = client.query_one(qry, &[]).await.unwrap();
    MiniHeader {
        height: row.get(0),
        timestamp: row.get(1),
        id: row.get(2),
    }
}

pub async fn insert(pgtx: &Transaction<'_>, header: &MiniHeader) {
    let stmt = "
        insert into sigmausd.headers (height, timestamp, id)
        values ($1, $2, $3);";
    pgtx.execute(stmt, &[&header.height, &header.timestamp, &header.id])
        .await
        .unwrap();
}

pub async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    pgtx.execute(
        "delete from sigmausd.headers where height = $1;",
        &[&height],
    )
    .await
    .unwrap();
}

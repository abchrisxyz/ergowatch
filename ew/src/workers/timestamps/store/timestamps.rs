use tokio_postgres::Transaction;

use crate::core::types::Height;
use crate::core::types::Timestamp;

pub async fn get_at(pgtx: &Transaction<'_>, height: Height) -> Timestamp {
    tracing::trace!("get_at {height}");
    let qry = "
        select timestamp
        from timestamps.timestamps
        where height = $1;";
    pgtx.query_one(qry, &[&height])
        .await
        .unwrap()
        .get::<usize, Timestamp>(0)
}

pub async fn insert(pgtx: &Transaction<'_>, height: Height, timestamp: Timestamp) {
    tracing::trace!("insert {height} {timestamp}");
    let stmt = "
        insert into timestamps.timestamps (height, timestamp)
        values ($1, $2);";
    pgtx.execute(stmt, &[&height, &timestamp]).await.unwrap();
}

pub async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    pgtx.execute(
        "delete from timestamps.timestamps where height = $1;",
        &[&height],
    )
    .await
    .unwrap();
}

use tokio_postgres::Transaction;

use crate::core::types::Height;
use crate::core::types::Timestamp;

pub async fn insert(pgtx: &Transaction<'_>, height: Height, timestamp: Timestamp) {
    tracing::trace!("insert {height} {timestamp}");
    let stmt = "
        insert into erg.timestamps (height, timestamp)
        values ($1, $2);";
    pgtx.execute(stmt, &[&height, &timestamp]).await.unwrap();
}

pub async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    pgtx.execute("delete from erg.timestamps where height = $1;", &[&height])
        .await
        .unwrap();
}

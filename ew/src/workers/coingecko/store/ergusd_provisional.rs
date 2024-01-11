use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::ProvisionalBlockRecord;
use crate::core::types::Height;

pub(super) async fn insert(pgtx: &Transaction<'_>, record: &ProvisionalBlockRecord) {
    tracing::trace!("insert {record:?}");
    let sql = "insert into coingecko.ergusd_provisional_blocks(height, timestamp) values ($1, $2);";
    pgtx.execute(sql, &[&record.height, &record.timestamp])
        .await
        .unwrap();
}

pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    let sql = "delete from coingecko.ergusd_provisional_blocks where height = $1;";
    pgtx.execute(sql, &[&height]).await.unwrap();
}

pub(super) async fn delete_many_at(pgtx: &Transaction<'_>, heights: &Vec<Height>) {
    tracing::trace!("delete_updated {heights:?}");
    let sql = "delete from coingecko.ergusd_provisional_blocks where height = any($1);";
    pgtx.execute(sql, &[&heights]).await.unwrap();
}

pub(super) async fn get_all(client: &Client) -> Vec<ProvisionalBlockRecord> {
    let sql = "
        select height
            , timestamp
        from coingecko.ergusd_provisional_blocks
        order by 1;
    ";
    client
        .query(sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| ProvisionalBlockRecord {
            height: row.get(0),
            timestamp: row.get(1),
        })
        .collect()
}

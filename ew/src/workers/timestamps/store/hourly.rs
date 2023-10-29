use crate::core::types::Height;
use crate::core::types::Timestamp;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::TimestampRecord;

/// Returns record with latest timestamp, if any
pub(super) async fn get_last(client: &Client) -> Option<TimestampRecord> {
    let qry = "
        select height
            , timestamp
        from timestamps.hourly
        order by timestamp desc
        limit 1;";
    client
        .query_opt(qry, &[])
        .await
        .unwrap()
        .map(|row| TimestampRecord {
            height: row.get(0),
            timestamp: row.get(1),
        })
}

/// Insert
pub(super) async fn insert(pgtx: &Transaction<'_>, record: &TimestampRecord) {
    let stmt = "insert into timestamps.hourly (height, timestamp) values ($1, $2);";
    pgtx.execute(stmt, &[&record.height, &record.timestamp])
        .await
        .unwrap();
}

/// Update timestamp of record with same height.
pub(super) async fn update(pgtx: &Transaction<'_>, record: &TimestampRecord) {
    let stmt = "
        update timestamps.hourly
        set timestamp = $2
        where height = $1;";
    pgtx.execute(stmt, &[&record.height, &record.timestamp])
        .await
        .unwrap();
}

/// Delete records at `height`.
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: &Height) {
    let stmt = "
        delete from timestamps.hourly
        where height = $1;";
    pgtx.execute(stmt, &[height]).await.unwrap();
}

/// Delete all records with a timestamp after `timestamp`
pub(super) async fn delete_after(pgtx: &Transaction<'_>, timestamp: Timestamp) {
    let stmt = "
        delete from timestamps.hourly
        where timestamp > $1;";
    pgtx.execute(stmt, &[&timestamp]).await.unwrap();
}

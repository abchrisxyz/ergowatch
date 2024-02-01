use crate::core::types::Height;
use tokio_postgres::Transaction;

use super::super::types::VotesRecord;

/// Insert a record
pub(super) async fn insert(pgtx: &Transaction<'_>, record: &VotesRecord) {
    tracing::trace!("insert {record:?}");
    let stmt = "
        insert into network.votes (height, slots)
        values ($1, array[$2, $3, $4]::smallint[]);";
    pgtx.execute(
        stmt,
        &[&record.height, &record.slot1, &record.slot2, &record.slot3],
    )
    .await
    .unwrap();
}

/// Get votes for given `height`
pub(super) async fn get_at(pgtx: &Transaction<'_>, height: Height) -> Option<VotesRecord> {
    tracing::trace!("get_at {height}");
    let stmt = "
        select height
            , slots[1]
            , slots[2]
            , slots[3]
        from network.votes
        where height = $1";
    pgtx.query_opt(stmt, &[&height])
        .await
        .unwrap()
        .and_then(|row| {
            Some(VotesRecord {
                height: row.get(0),
                slot1: row.get(1),
                slot2: row.get(2),
                slot3: row.get(3),
            })
        })
}

/// Delete record for given `height`
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    let sql = "delete from network.votes where height = $1;";
    pgtx.execute(sql, &[&height]).await.unwrap();
}

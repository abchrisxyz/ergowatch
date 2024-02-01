use crate::core::types::Height;
use tokio_postgres::Transaction;

use super::super::types::TransactionsRecord;

/// Insert a record
pub(super) async fn insert(pgtx: &Transaction<'_>, record: &TransactionsRecord) {
    tracing::trace!("insert {record:?}");
    let stmt = "
        insert into network.transactions (
            height,
            transactions,
            user_transactions
        )
        values ($1, $2, $3);";
    pgtx.execute(
        stmt,
        &[
            &record.height,
            &record.transactions,
            &record.user_transactions,
        ],
    )
    .await
    .unwrap();
}

/// Delete record for given `height`
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    let sql = "delete from network.transactions where height = $1;";
    pgtx.execute(sql, &[&height]).await.unwrap();
}

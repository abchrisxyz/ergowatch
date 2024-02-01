use crate::core::types::Height;
use tokio_postgres::types::Type;
use tokio_postgres::Transaction;

use super::super::types::UnhandledExtensionRecord;

/// Insert a record
pub(super) async fn insert_many(pgtx: &Transaction<'_>, records: &[UnhandledExtensionRecord]) {
    tracing::trace!("insert_many {records:?}");
    if records.is_empty() {
        return;
    }
    let sql = "
        insert into network._unhandled_extension_fields (
            height,
            key,
            value_base16
        )
        values ($1, $2, $3);";
    let stmt = pgtx
        .prepare_typed(sql, &[Type::INT4, Type::INT2, Type::TEXT])
        .await
        .unwrap();
    for record in records {
        pgtx.execute(&stmt, &[&record.height, &record.key, &record.value])
            .await
            .unwrap();
    }
}

/// Delete records for given `height`
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    let sql = "delete from network._unhandled_extension_fields where height = $1;";
    pgtx.execute(sql, &[&height]).await.unwrap();
}

use tokio_postgres::types::Type;
use tokio_postgres::Transaction;

use super::super::types::BlockRecord;
use crate::core::types::Height;

pub(super) async fn insert(pgtx: &Transaction<'_>, record: &BlockRecord) {
    tracing::trace!("insert {record:?}");
    let sql = "insert into coingecko.ergusd_block (height, value) values ($1, $2);";
    pgtx.execute(sql, &[&record.height, &record.usd])
        .await
        .unwrap();
}

pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    let sql = "delete from coingecko.ergusd_block where height = $1;";
    pgtx.execute(sql, &[&height]).await.unwrap();
}

pub(super) async fn update_many(pgtx: &Transaction<'_>, records: &Vec<BlockRecord>) {
    tracing::trace!("update_many {records:?}");
    let sql = "
        update coingecko.ergusd_block
        set value = $2
        where height = $1;
    ";
    let stmt = pgtx
        .prepare_typed(sql, &[Type::INT4, Type::FLOAT4])
        .await
        .unwrap();
    for rec in records {
        pgtx.execute(&stmt, &[&rec.height, &rec.usd]).await.unwrap();
    }
}

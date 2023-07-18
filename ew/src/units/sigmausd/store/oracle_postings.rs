use tokio_postgres::Transaction;

use super::super::types::OraclePosting;
use crate::core::types::Height;

pub async fn insert(pgtx: &Transaction<'_>, op: &OraclePosting) {
    let stmt = "
        insert into sigmausd.oracle_postings (height, datapoint, box_id)
        values ($1, $2, $3);
        ";
    pgtx.execute(stmt, &[&op.height, &op.datapoint, &op.box_id])
        .await;
}

pub async fn delete_at(pgtx: &Transaction<'_>, at: Height) {
    let stmt = "delete from sigmausd.oracle_postings where height = $1;";
    pgtx.execute(stmt, &[&at]).await;
}

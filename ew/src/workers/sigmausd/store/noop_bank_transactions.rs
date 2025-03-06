use tokio_postgres::Transaction;

use crate::core::types::Height;

use super::super::types::NoopBankTransaction;

pub(super) async fn insert(pgtx: &Transaction<'_>, ntx: &NoopBankTransaction) {
    let sql = "
        insert into sigmausd.noop_bank_transactions (
            height,
            tx_idx,
            tx_id,
            box_id
        )
        values ($1, $2, $3, $4);
    ";
    pgtx.execute(sql, &[&ntx.height, &ntx.tx_idx, &ntx.tx_id, &ntx.box_id])
        .await
        .unwrap();
}

/// Delete records at `height`.
pub(super) async fn detele_at(pgtx: &Transaction<'_>, height: Height) {
    let sql = "delete from sigmausd.noop_bank_transactions where height = $1;";

    pgtx.execute(sql, &[&height]).await.unwrap();
}

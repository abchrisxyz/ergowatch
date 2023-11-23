use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::Height;

use super::super::types::BankTransaction;

pub(super) async fn get_count(client: &Client) -> i32 {
    let sql = "select count(*) from sigmausd.bank_transactions;";
    client
        .query_one(sql, &[])
        .await
        .unwrap()
        .get::<usize, i64>(0) as i32
}

pub(super) async fn insert(pgtx: &Transaction<'_>, btx: &BankTransaction) {
    let sql = "
        insert into sigmausd.bank_transactions (
            idx,
            height,
            timestamp,
            reserves_diff,
            circ_sc_diff,
            circ_rc_diff,
            box_id,
            service_fee,
            service_address_id
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9);
    ";
    pgtx.execute(
        sql,
        &[
            &btx.index,
            &btx.height,
            &btx.timestamp,
            &btx.reserves_diff,
            &btx.circ_sc_diff,
            &btx.circ_rc_diff,
            &btx.box_id,
            &btx.service_fee,
            &btx.service_address_id,
        ],
    )
    .await
    .unwrap();
}

/// Delete records at `height`.
pub(super) async fn detele_at(pgtx: &Transaction<'_>, height: Height) {
    let sql = "delete from sigmausd.bank_transactions where height = $1;";

    pgtx.execute(sql, &[&height]).await.unwrap();
}

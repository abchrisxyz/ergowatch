use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::HistoryRecord;
use crate::core::types::Height;

pub(super) async fn get_latest(client: &Client) -> HistoryRecord {
    let sql = "
        select height
            , oracle
            , circ_sc
            , circ_rc
            , reserves
            , sc_nano_net
            , rc_nano_net
        from sigmausd.history
        order by height desc limit 1;
    ";
    let row = client.query_one(sql, &[]).await.unwrap();
    HistoryRecord {
        height: row.get(0),
        oracle: row.get(1),
        circ_sc: row.get(2),
        circ_rc: row.get(3),
        reserves: row.get(4),
        sc_net: row.get(5),
        rc_net: row.get(6),
    }
}

pub(super) async fn insert(pgtx: &Transaction<'_>, hr: &HistoryRecord) {
    let sql = "
        insert into sigmausd.history (
            height,
            oracle,
            circ_sc,
            circ_rc,
            reserves,
            sc_nano_net,
            rc_nano_net,
        ) values ($1, $2, $3, $4, $5, $6, $7);
    ";
    pgtx.execute(
        sql,
        &[
            &hr.height,
            &hr.oracle,
            &hr.circ_sc,
            &hr.circ_rc,
            &hr.reserves,
            &hr.sc_net,
            &hr.rc_net,
        ],
    )
    .await
    .unwrap();
}

pub(super) async fn delete_at(pgtx: &Transaction<'_>, at: Height) {
    let sql = "delete from sigmausd.history where height = $1;";
    pgtx.execute(sql, &[&at]).await.unwrap();
}

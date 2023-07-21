use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::OraclePosting;
use crate::core::types::Height;

pub async fn insert(pgtx: &Transaction<'_>, op: &OraclePosting) {
    let stmt = "
        insert into sigmausd.oracle_postings (height, datapoint, box_id)
        values ($1, $2, $3);
        ";
    pgtx.execute(stmt, &[&op.height, &op.datapoint, &op.box_id])
        .await
        .unwrap();
}

pub async fn delete_at(pgtx: &Transaction<'_>, at: Height) {
    let stmt = "delete from sigmausd.oracle_postings where height = $1;";
    pgtx.execute(stmt, &[&at]).await.unwrap();
}

pub async fn get_latest(client: &Client) -> OraclePosting {
    let qry = "
        select
            height,
            datapoint,
            box_id
        from sigmausd.oracle_postings
        order by 1 desc
        limit 1;
        ";
    match client.query_opt(qry, &[]).await.unwrap() {
        Some(row) => OraclePosting {
            height: row.get(0),
            datapoint: row.get(1),
            box_id: row.get(2),
        },
        None => OraclePosting {
            height: 0,
            datapoint: 0,
            box_id: String::new(),
        },
    }
}

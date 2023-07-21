use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::Head;

pub async fn get(client: &Client) -> Head {
    let qry = "select height, header_id from sigmausd.head;";
    match client.query_opt(qry, &[]).await.unwrap() {
        Some(row) => Head::new(row.get(0), row.get(1)),
        None => Head::initial(),
    }
}
pub async fn update(pgtx: &Transaction<'_>, head: &Head) {
    let stmt = "insert into sigmausd.head (height, header_id) values ($1, $2);";
    pgtx.execute(stmt, &[&head.height, &head.header_id]).await;
}

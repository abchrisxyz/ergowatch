use tokio_postgres::Client;
use tokio_postgres::Transaction;

pub(super) async fn get_count(client: &Client) -> i32 {
    let sql = "select count(*) from sigmausd.bank_transactions;";
    client.query_one(sql, &[]).await.unwrap().get(0)
}

use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::HistoryRecord;

pub(super) async fn get_latest(client: &Client) -> HistoryRecord {
    todo!()
}

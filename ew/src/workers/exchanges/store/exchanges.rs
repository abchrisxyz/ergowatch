use tokio_postgres::GenericClient;

use super::super::types::ExchangeRecord;

pub(super) async fn insert(client: &impl GenericClient, record: &ExchangeRecord) {
    tracing::trace!("insert {record:?}");
    let sql = "
        insert into exchanges.exchanges (id, text_id, name)
        values ($1, $2, $3);
    ";
    client
        .execute(sql, &[&record.id, &record.text_id, &record.name])
        .await
        .unwrap();
}

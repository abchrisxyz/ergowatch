use crate::core::types::Height;
use tokio_postgres::Transaction;

use super::super::types::NetworkParametersRecord;

/// Insert a record
pub(super) async fn insert(pgtx: &Transaction<'_>, record: &NetworkParametersRecord) {
    tracing::trace!("insert {record:?}");
    let stmt = "
        insert into network.parameters (
            height,
            storage_fee,
            min_box_value,
            max_block_size,
            max_cost,
            token_access_cost,
            tx_input_cost,
            tx_data_input_cost,
            tx_output_cost,
            block_version
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);";
    pgtx.execute(
        stmt,
        &[
            &record.height,
            &record.storage_fee,
            &record.min_box_value,
            &record.max_block_size,
            &record.max_cost,
            &record.token_access_cost,
            &record.tx_input_cost,
            &record.tx_data_input_cost,
            &record.tx_output_cost,
            &record.block_version,
        ],
    )
    .await
    .unwrap();
}

/// Delete record for given `height`
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    let sql = "delete from network.parameters where height = $1;";
    pgtx.execute(sql, &[&height]).await.unwrap();
}

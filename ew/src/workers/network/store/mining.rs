use crate::core::types::Height;
use tokio_postgres::Transaction;

use super::super::types::MiningRecord;

/// Insert a record
pub(super) async fn insert(pgtx: &Transaction<'_>, record: &MiningRecord) {
    tracing::trace!("insert {record:?}");
    let stmt = "
        insert into network.mining (
            height,
            miner_address_id,
            difficulty,
            difficulty_24h_mean,
            hash_rate_24h_mean,
            block_reward,
            tx_fees
        )
        values ($1, $2, $3, $4, $5, $6, $7);";
    pgtx.execute(
        stmt,
        &[
            &record.height,
            &record.miner_address_id,
            &record.difficulty,
            &record.difficulty_24h_mean,
            &record.hash_rate_24h_mean,
            &record.block_reward,
            &record.tx_fees,
        ],
    )
    .await
    .unwrap();
}

/// Delete record for given `height`
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    let sql = "delete from network.mining where height = $1;";
    pgtx.execute(sql, &[&height]).await.unwrap();
}

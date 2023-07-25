use tokio_postgres::Transaction;

use super::super::types::ServiceStats;

pub(super) async fn upsert(pgtx: &Transaction<'_>, diff: &ServiceStats) {
    let sql = "
        insert into sigmausd.services (
            address_id,
            tx_count,
            first_tx,
            last_tx,
            fees,
            volume
        ) values ($1, $2, $3, $4, $5, $6)
        on conflict (address_id) do update
            set tx_count = tx_count + EXCLUDED.tx_count,
            set last_tx = EXCLUDED.last_tx,
            set fees = fees + EXCLUDED.fees,
            set volume = volume + EXCLUDED.volume
        ;
    ";
    pgtx.execute(
        sql,
        &[
            &diff.address_id,
            &diff.tx_count,
            &diff.first_tx,
            &diff.last_tx,
            &diff.fees,
            &diff.volume,
        ],
    )
    .await
    .unwrap();
}

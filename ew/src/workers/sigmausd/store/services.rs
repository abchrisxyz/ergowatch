use tokio_postgres::Transaction;

use super::super::types::ServiceStats;

pub(super) async fn upsert(pgtx: &Transaction<'_>, diff: &ServiceStats) {
    let sql = "
        insert into sigmausd.services as t (
            address_id,
            tx_count,
            first_tx,
            last_tx,
            fees,
            volume
        ) values ($1, $2, $3, $4, $5, $6)
        on conflict (coalesce(address_id, 0)) do update
        set tx_count = t.tx_count + EXCLUDED.tx_count
            , last_tx = EXCLUDED.last_tx
            , fees = t.fees + EXCLUDED.fees
            , volume = t.volume + EXCLUDED.volume
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

/// Truncates and repopulates service stats from a bank transactions query.
pub(super) async fn refresh(pgtx: &Transaction<'_>) {
    pgtx.execute("truncate sigmausd.services;", &[])
        .await
        .unwrap();
    let qry = "
        insert into sigmausd.services (
            address_id,
            tx_count,
            first_tx,
            last_tx,
            fees,
            volume
        )
        select service_address_id
            , count(*)
            , min(timestamp) as first_tx
            , max(timestamp) as last_tx
            , sum(service_fee) as fees
            , sum(abs(reserves_diff)) as volume
        from sigmausd.bank_transactions
        group by 1
        -- order by first_tx to reproduce order of appearance
        order by 3;
    ";
    pgtx.execute(qry, &[]).await.unwrap();
}

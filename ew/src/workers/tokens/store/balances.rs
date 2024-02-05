use tokio_postgres::types::Type;
use tokio_postgres::GenericClient;
use tokio_postgres::Transaction;

use super::super::types::AddressAsset;
use super::super::types::BalanceRecord;

/// Get collection of balance records for given `address_ids`.
pub async fn get_many(
    client: &impl GenericClient,
    address_assets: &Vec<AddressAsset>,
) -> Vec<BalanceRecord> {
    tracing::trace!("get_many {address_assets:?}");
    if address_assets.is_empty() {
        return vec![];
    }
    let sql = format!(
        "
        select address_id
            , asset_id
            , value
        from tokens.balances
        where (address_id, asset_id) = any(values {});",
        address_assets
            .iter()
            .map(|aa| format!("({},{})", aa.0 .0, aa.1))
            .collect::<Vec<_>>()
            .join(",")
    );
    client
        .query(&sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|r| BalanceRecord::new(r.get(0), r.get(1), r.get(2)))
        .collect()
}

/// Upsert collection of balance records.
pub async fn upsert_many(pgtx: &Transaction<'_>, records: &Vec<BalanceRecord>) {
    tracing::trace!("upsert_many {records:?}");
    let sql = "
        insert into tokens.balances (address_id, asset_id, value)
        values ($1, $2, $3)
        on conflict (address_id, asset_id) do update
        set value = EXCLUDED.value
        ;";
    let stmt = pgtx
        .prepare_typed(sql, &[Type::INT8, Type::INT8, Type::INT8])
        .await
        .unwrap();
    for r in records {
        pgtx.execute(&stmt, &[&r.address_id, &r.asset_id, &r.value])
            .await
            .unwrap();
    }
}

/// Delete balances for given address id's.
pub async fn delete_many(pgtx: &Transaction<'_>, address_assets: &Vec<AddressAsset>) {
    tracing::trace!("delete_many {address_assets:?}");
    if address_assets.is_empty() {
        return;
    }
    let sql = format!(
        "delete from tokens.balances where (address_id, asset_id) = any(values {});",
        address_assets
            .iter()
            .map(|aa| format!("({},{})", aa.0 .0, aa.1))
            .collect::<Vec<_>>()
            .join(",")
    );
    pgtx.execute(&sql, &[]).await.unwrap();
}

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
            , mean_age_timestamp
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
        .map(|r| BalanceRecord::new(r.get(0), r.get(1), r.get(2), r.get(3)))
        .collect()
}

/// Upsert collection of balance records.
pub async fn upsert_many(pgtx: &Transaction<'_>, records: &Vec<BalanceRecord>) {
    tracing::trace!("upsert_many {records:?}");
    let sql = "
        insert into tokens.balances (address_id, asset_id, value, mean_age_timestamp)
        values ($1, $2, $3, $4)
        on conflict (address_id, asset_id) do update
        set value = EXCLUDED.value
            , mean_age_timestamp = EXCLUDED.mean_age_timestamp
        ;";
    let stmt = pgtx
        .prepare_typed(sql, &[Type::INT8, Type::INT8, Type::INT8, Type::INT8])
        .await
        .unwrap();
    for r in records {
        pgtx.execute(
            &stmt,
            &[&r.address_id, &r.asset_id, &r.value, &r.mean_age_timestamp],
        )
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

pub mod logs {
    use super::*;
    use crate::core::types::Height;

    /// Log given `address_assets` as being created at `height`.
    ///
    /// Must be called prior to modifying balances for `height`.
    pub async fn log_new_balances(
        pgtx: &Transaction<'_>,
        height: Height,
        address_assets: &Vec<AddressAsset>,
    ) {
        tracing::trace!("log_new_balances {height} {address_assets:?}");
        if address_assets.is_empty() {
            return;
        }
        let sql = format!(
            "
            insert into tokens._log_balances_created_at(height, address_id, asset_id)
            values {};",
            address_assets
                .iter()
                .map(|aa| format!("({height},{},{})", aa.0 .0, aa.1))
                .collect::<Vec<_>>()
                .join(",")
        );
        pgtx.execute(&sql, &[]).await.unwrap();
    }

    /// Get `address_assets` that where created at `height`.
    pub async fn get_address_assets_created_at(
        pgtx: &Transaction<'_>,
        height: Height,
    ) -> Vec<AddressAsset> {
        tracing::trace!("get_address_assets_created_at {height}");
        let sql = "
            select address_id
                , asset_id
            from tokens._log_balances_created_at
            where height = $1;
        ";
        pgtx.query(sql, &[&height])
            .await
            .unwrap()
            .iter()
            .map(|r| AddressAsset(r.get(0), r.get(1)))
            .collect()
    }

    /// Log state of existing balances for given `address_ids`.
    ///
    /// Must be called prior to modifying balances for `height`.
    pub async fn log_existing_balances(
        pgtx: &Transaction<'_>,
        height: Height,
        address_assets: &Vec<AddressAsset>,
    ) {
        tracing::trace!("log_existing_balances {height} {address_assets:?}");
        if address_assets.is_empty() {
            return;
        }
        let sql = format!(
            "
            insert into tokens._log_balances_previous_state_at(
                height,
                address_id,
                asset_id,
                value,
                mean_age_timestamp
            )
            select $1
                , address_id
                , asset_id
                , value
                , mean_age_timestamp
            from tokens.balances
            where (address_id, asset_id) = any(values {});
            ;",
            address_assets
                .iter()
                .map(|aa| format!("({},{})", aa.0 .0, aa.1))
                .collect::<Vec<_>>()
                .join(",")
        );
        pgtx.execute(&sql, &[&height]).await.unwrap();
    }

    /// Get balances logged at `height`.
    pub async fn get_balances_at(pgtx: &Transaction<'_>, height: Height) -> Vec<BalanceRecord> {
        tracing::trace!("get_balances_at {height}");
        let sql = "
            select address_id
                , asset_id
                , value
                , mean_age_timestamp
            from tokens._log_balances_previous_state_at
            where height = $1;
        ";
        let balances: Vec<BalanceRecord> = pgtx
            .query(sql, &[&height])
            .await
            .unwrap()
            .iter()
            .map(|r| BalanceRecord {
                address_id: r.get(0),
                asset_id: r.get(1),
                value: r.get(2),
                mean_age_timestamp: r.get(3),
            })
            .collect();
        if balances.is_empty() {
            panic!("Rollback horizon reached!");
        }
        balances
    }

    /// Delete log records prior to given `height`.
    pub async fn delete_logs_prior_to(pgtx: &Transaction<'_>, height: Height) {
        tracing::trace!("delete_logs_prior_to {height}");

        let sql = "delete from tokens._log_balances_previous_state_at where height < $1;";
        pgtx.execute(sql, &[&height]).await.unwrap();

        let sql = "delete from tokens._log_balances_created_at where height < $1;";
        pgtx.execute(sql, &[&height]).await.unwrap();
    }

    /// Delete log records for given `height`.
    pub async fn delete_logs_at(pgtx: &Transaction<'_>, height: Height) {
        tracing::trace!("delete_logs_at {height}");

        let sql = "delete from tokens._log_balances_previous_state_at where height = $1;";
        pgtx.execute(sql, &[&height]).await.unwrap();

        let sql = "delete from tokens._log_balances_created_at where height = $1;";
        pgtx.execute(sql, &[&height]).await.unwrap();
    }
}

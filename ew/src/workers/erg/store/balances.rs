use tokio_postgres::types::Type;
use tokio_postgres::GenericClient;
use tokio_postgres::Transaction;

use super::super::types::BalanceRecord;
use crate::core::types::AddressID;

/// Get collection of balance records for given `address_ids`.
pub async fn get_many(
    client: &impl GenericClient,
    address_ids: &Vec<AddressID>,
) -> Vec<BalanceRecord> {
    tracing::trace!("get_many {address_ids:?}");
    let sql = "
        select address_id
            , nano
            , mean_age_timestamp
        from erg.balances
        where address_id = any($1);";
    client
        .query(sql, &[&address_ids])
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
        insert into erg.balances (address_id, nano, mean_age_timestamp)
        values ($1, $2, $3)
        on conflict (address_id) do update
        set nano = EXCLUDED.nano
            , mean_age_timestamp = EXCLUDED.mean_age_timestamp
        ;";
    let stmt = pgtx
        .prepare_typed(sql, &[Type::INT8, Type::INT8, Type::INT8])
        .await
        .unwrap();
    for r in records {
        pgtx.execute(&stmt, &[&r.address_id, &r.nano, &r.mean_age_timestamp])
            .await
            .unwrap();
    }
}

/// Delete balances for given address id's.
pub async fn delete_many(pgtx: &Transaction<'_>, address_ids: &Vec<AddressID>) {
    tracing::trace!("delete_many {address_ids:?}");
    pgtx.execute(
        "delete from erg.balances where address_id = any($1);",
        &[&address_ids],
    )
    .await
    .unwrap();
}

pub mod logs {
    use super::*;
    use crate::core::types::Height;

    /// Log given `address_ids` as being created at `height`.
    ///
    /// Must be called prior to modifying balances for `height`.
    pub async fn log_new_balances(
        pgtx: &Transaction<'_>,
        height: Height,
        address_ids: &Vec<AddressID>,
    ) {
        tracing::trace!("log_new_balances {height} {address_ids:?}");
        let sql = "
            insert into erg._log_balances_created_at(height, address_id)
            select $1, unnest($2::bigint[]);";
        pgtx.execute(sql, &[&height, &address_ids]).await.unwrap();
    }

    /// Get `address_ids` that where created at `height`.
    pub async fn get_addresses_created_at(
        pgtx: &Transaction<'_>,
        height: Height,
    ) -> Vec<AddressID> {
        tracing::trace!("get_addresses_created_at {height}");
        let sql = "
            select address_id
            from erg._log_balances_created_at
            where height = $1;
        ";
        pgtx.query(sql, &[&height])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    /// Log state of existing balances for given `address_ids`.
    ///
    /// Must be called prior to modifying balances for `height`.
    pub async fn log_existing_balances(
        pgtx: &Transaction<'_>,
        height: Height,
        address_ids: &Vec<AddressID>,
    ) {
        tracing::trace!("log_existing_balances {height} {address_ids:?}");
        let sql = "
            insert into erg._log_balances_previous_state_at(
                height,
                address_id,
                nano,
                mean_age_timestamp
            )
            select $1
                , address_id
                , nano
                , mean_age_timestamp
            from erg.balances
            where address_id = any($2);
            ;";
        pgtx.execute(sql, &[&height, &address_ids]).await.unwrap();
    }

    /// Get balances logged at `height`.
    pub async fn get_balances_at(pgtx: &Transaction<'_>, height: Height) -> Vec<BalanceRecord> {
        tracing::trace!("get_balances_at {height}");
        let sql = "
            select address_id
                , nano
                , mean_age_timestamp
            from erg._log_balances_previous_state_at
            where height = $1;
        ";
        let balances: Vec<BalanceRecord> = pgtx
            .query(sql, &[&height])
            .await
            .unwrap()
            .iter()
            .map(|r| BalanceRecord {
                address_id: r.get(0),
                nano: r.get(1),
                mean_age_timestamp: r.get(2),
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

        let sql = "delete from erg._log_balances_previous_state_at where height < $1;";
        pgtx.execute(sql, &[&height]).await.unwrap();

        let sql = "delete from erg._log_balances_created_at where height < $1;";
        pgtx.execute(sql, &[&height]).await.unwrap();
    }

    /// Delete log records for given `height`.
    pub async fn delete_logs_at(pgtx: &Transaction<'_>, height: Height) {
        tracing::trace!("delete_logs_at {height}");

        let sql = "delete from erg._log_balances_previous_state_at where height = $1;";
        pgtx.execute(sql, &[&height]).await.unwrap();

        let sql = "delete from erg._log_balances_created_at where height = $1;";
        pgtx.execute(sql, &[&height]).await.unwrap();
    }
}

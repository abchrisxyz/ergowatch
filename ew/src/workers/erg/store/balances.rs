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
    pgtx.execute(
        "delete from erg.balances where address_id = any($1);",
        &[&address_ids],
    )
    .await
    .unwrap();
}

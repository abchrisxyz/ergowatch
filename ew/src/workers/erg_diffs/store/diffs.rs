use tokio_postgres::types::Type;
use tokio_postgres::Client;
use tokio_postgres::GenericClient;
use tokio_postgres::Transaction;

use super::super::types::DiffRecord;
use super::super::types::SupplyDiff;
use crate::core::types::AddressID;
use crate::core::types::Height;

/// Insert collection of diff records.
pub async fn insert_many(pgtx: &Transaction<'_>, records: &Vec<DiffRecord>) {
    tracing::trace!("insert_many {records:?}");
    let sql = "
        insert into erg.balance_diffs (address_id, height, tx_idx, nano)
        values ($1, $2, $3, $4);";
    let stmt = pgtx
        .prepare_typed(sql, &[Type::INT8, Type::INT4, Type::INT2, Type::INT8])
        .await
        .unwrap();
    for r in records {
        pgtx.execute(&stmt, &[&r.address_id, &r.height, &r.tx_idx, &r.nano])
            .await
            .unwrap();
    }
}

/// Delete diff records at given `height`.
pub async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    pgtx.execute(
        "delete from erg.balance_diffs where height = $1;",
        &[&height],
    )
    .await
    .unwrap();
}

/// Get diff records for given `height`.
pub async fn select_slice(
    client: &impl GenericClient,
    ge_height: Height,
    le_height: Height,
) -> Vec<DiffRecord> {
    tracing::trace!("select_slice {ge_height} {le_height}");
    let rows = client
        .query(
            "select address_id
            , height
            , tx_idx
            , nano
        from erg.balance_diffs
        where height >= $1 and height <= $2;",
            &[&ge_height, &le_height],
        )
        .await
        .unwrap();
    rows.iter()
        .map(|r| DiffRecord {
            address_id: r.get(0),
            height: r.get(1),
            tx_idx: r.get(2),
            nano: r.get(3),
        })
        .collect()
}

/// Get aggregate series of balance diffs for given addresses,
/// up to and including `max_height`.
pub async fn select_aggregate_series(
    client: &Client,
    address_ids: &Vec<AddressID>,
    max_height: Height,
) -> Vec<SupplyDiff> {
    let sql = "
        select height
            , sum(nano)::bigint
        from erg.balance_diffs
        where address_id = any ($1) and height <= $2
        group by 1
        order by 1;
    ";
    client
        .query(sql, &[&address_ids, &max_height])
        .await
        .unwrap()
        .iter()
        .map(|r| SupplyDiff {
            height: r.get(0),
            nano: r.get(1),
        })
        .collect()
}

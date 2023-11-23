use tokio_postgres::types::Type;
use tokio_postgres::GenericClient;
use tokio_postgres::Transaction;

use super::super::types::DiffRecord;
use crate::core::types::AddressID;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;

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
pub async fn select_at(client: &impl GenericClient, height: Height) -> Vec<DiffRecord> {
    tracing::trace!("select_at {height}");
    let rows = client
        .query(
            "select address_id
            , height
            , tx_idx
            , nano
        from erg.balance_diffs
        where height = $1;",
            &[&height],
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

/// Retrieve all balance diffs for given address.
pub(super) async fn get_address_diffs(
    client: &impl GenericClient,
    address_id: AddressID,
) -> Vec<(NanoERG, Timestamp)> {
    tracing::trace!("get_address_diffs {address_id}");
    let sql = "
        select d.nano, t.timestamp
        from erg.balance_diffs d
        join erg.timestamps t on t.height = d.height
        where d.address_id = $1
        order by d.height, d.tx_idx;
    ";
    client
        .query(sql, &[&address_id])
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

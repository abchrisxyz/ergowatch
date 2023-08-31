use tokio_postgres::types::Type;
use tokio_postgres::Transaction;

use super::super::types::DiffRecord;
use crate::core::types::AddressID;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;

/// Insert collection of diff records.
pub async fn insert_many(pgtx: &Transaction<'_>, records: &Vec<DiffRecord>) {
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
    pgtx.execute(
        "delete from erg.balance_diffs where height = $1;",
        &[&height],
    )
    .await
    .unwrap();
}

/// Get diff records for given `height`.
pub async fn select_at(pgtx: &Transaction<'_>, height: Height) -> Vec<DiffRecord> {
    let rows = pgtx
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
    todo!()
}

pub(super) async fn get_address_diffs(
    pgtx: &Transaction<'_>,
    address_id: AddressID,
) -> Vec<(NanoERG, Timestamp)> {
    let sql = "
        select d.nano, h.timestamp
        from erg.balance_diffs d
        join erg.headers h on h.height = d.height
        where d.address_id = $1
        order by d.height, d.tx_idx;
    ";
    pgtx.query(sql, &[&address_id])
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

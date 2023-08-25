use tokio_postgres::types::Type;
use tokio_postgres::Transaction;

use super::super::types::DiffRecord;
use crate::core::types::Height;

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

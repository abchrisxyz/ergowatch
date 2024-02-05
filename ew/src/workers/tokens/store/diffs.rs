use tokio_postgres::types::Type;
use tokio_postgres::Transaction;

use super::super::types::AddressAsset;
use super::super::types::BalanceRecord;
use super::super::types::DiffRecord;
use crate::core::types::Height;

/// Insert collection of diff records.
pub async fn insert_many(pgtx: &Transaction<'_>, records: &Vec<DiffRecord>) {
    tracing::trace!("insert_many {records:?}");
    let sql = "
        insert into tokens.balance_diffs (address_id, asset_id, height, tx_idx, value)
        values ($1, $2, $3, $4, $5);";
    let stmt = pgtx
        .prepare_typed(
            sql,
            &[Type::INT8, Type::INT8, Type::INT4, Type::INT2, Type::INT8],
        )
        .await
        .unwrap();
    for r in records {
        pgtx.execute(
            &stmt,
            &[&r.address_id, &r.asset_id, &r.height, &r.tx_idx, &r.value],
        )
        .await
        .unwrap();
    }
}

/// Delete diff records at given `height`.
pub async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    pgtx.execute(
        "delete from tokens.balance_diffs where height = $1;",
        &[&height],
    )
    .await
    .unwrap();
}

/// Return all diff records for given `height`.
pub async fn get_many_at(pgtx: &Transaction<'_>, height: Height) -> Vec<DiffRecord> {
    tracing::trace!("get_many_at {height}");
    pgtx.query(
        "select address_id
            , asset_id
            , height
            , tx_idx
            , value
        from tokens.balance_diffs where height = $1;",
        &[&height],
    )
    .await
    .unwrap()
    .iter()
    .map(|r| DiffRecord {
        address_id: r.get(0),
        asset_id: r.get(1),
        height: r.get(2),
        tx_idx: r.get(3),
        value: r.get(4),
    })
    .collect()
}

/// Calculate balances from diffs for given address/asset pairs.
pub async fn get_balances_for(
    pgtx: &Transaction<'_>,
    address_assets: &[AddressAsset],
) -> Vec<BalanceRecord> {
    tracing::trace!("get_balances_for {address_assets:?}");
    if address_assets.is_empty() {
        return vec![];
    }
    let sql = format!(
        "
        select address_id
            , asset_id
            , sum(value)::bigint
        from tokens.balance_diffs
        where (address_id, asset_id) = any(values {})
        group by 1, 2
        ;",
        address_assets
            .iter()
            .map(|aa| format!("({},{})", aa.0 .0, aa.1))
            .collect::<Vec<_>>()
            .join(",")
    );
    pgtx.query(&sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|r| BalanceRecord {
            address_id: r.get(0),
            asset_id: r.get(1),
            value: r.get(2),
        })
        .collect()
}

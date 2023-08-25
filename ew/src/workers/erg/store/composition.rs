use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::CompositionRecord;
use crate::core::types::Height;

pub(super) async fn get_last(client: &Client) -> CompositionRecord {
    let sql = "
        select height
            , p2pks
            , contracts
            , miners
        from erg.supply_composition
        order by height desc
        limit 1;
    ";
    match client.query_opt(sql, &[]).await.unwrap() {
        Some(row) => CompositionRecord {
            height: row.get(0),
            p2pks: row.get(1),
            contracts: row.get(2),
            miners: row.get(3),
        },
        None => CompositionRecord {
            height: 0,
            p2pks: 0,
            contracts: 0,
            miners: 0,
        },
    }
}

pub(super) async fn insert(pgtx: &Transaction<'_>, record: &CompositionRecord) {
    let sql = "
        insert into erg.supply_composition (height, p2pks, contracts, miners)
        values ($1, $2, $3, $4)
    ";
    pgtx.execute(
        sql,
        &[
            &record.height,
            &record.p2pks,
            &record.contracts,
            &record.miners,
        ],
    )
    .await
    .unwrap();
}

/// Delete record for given `height`.
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    let sql = "delete from erg.supply_composition where height = $1;";
    pgtx.execute(sql, &[&height]).await.unwrap();
}

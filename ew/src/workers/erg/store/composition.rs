use tokio_postgres::Client;

use super::super::types::CompositionRecord;

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

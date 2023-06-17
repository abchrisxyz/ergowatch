use std::collections::HashMap;
use std::collections::HashSet;
use tokio_postgres::Transaction;

use crate::core::node;
use crate::core::types::AddressID;
use crate::core::types::BoxID;
use crate::core::types::Digest32;
use crate::core::types::Height;
use crate::core::types::Input;
use crate::core::types::Timestamp;

pub struct BoxIndex<'a> {
    pub box_id: &'a Digest32,
    pub height: Height,
    pub tx_index: i32,
    pub output_index: i32,
}

/// Intermediary (data)-input box data.
pub struct UTxO {
    pub height: Height,
    pub timestamp: Timestamp,
    pub output: node::models::Output,
}

pub(super) async fn insert_many<'a>(pgtx: &Transaction<'_>, records: Vec<BoxIndex<'a>>) {
    let sql = "insert into core.boxes (
        box_id,
        height,
        tx_index,
        output_index
    ) values ($1, $2, $3, $4);";
    let stmt = pgtx.prepare(sql).await.unwrap();
    for r in records {
        pgtx.execute(&stmt, &[&r.box_id, &r.height, &r.tx_index, &r.output_index])
            .await
            .unwrap();
    }
}
/// Returns collection of UTxO's ordered as in `box_ids`
pub(super) async fn get_boxes(pgtx: &Transaction<'_>, box_ids: Vec<BoxID>) -> Vec<UTxO> {
    tracing::debug!("retrieving input boxes");
    let qry = "
        select bx.height
            , bk.block -> 'header' -> 'timestamp'
            , bk.block -> 'blockTransactions' -> 'transactions' -> bx.tx_index -> 'outputs' -> bx.output_index as box
        from core.boxes bx
        join core.blocks bk on bk.height = bx.height
        where bx.box_id = any($1);";
    let rows = pgtx.query(qry, &[&box_ids]).await.unwrap();
    rows.into_iter()
        .map(|row| UTxO {
            height: row.get(0),
            timestamp: serde_json::from_value(row.get(1)).unwrap(),
            output: serde_json::from_value(row.get(2)).unwrap(),
        })
        .collect()
}

/// Maps `box_ids` to corresponding UTxO's.
pub(super) async fn map_boxes(
    pgtx: &Transaction<'_>,
    box_ids: HashSet<BoxID>,
) -> HashMap<BoxID, UTxO> {
    tracing::debug!("mapping input boxes");
    let mut map: HashMap<BoxID, UTxO> = HashMap::new();
    let qry = "
        select bx.height
            , bk.block -> 'header' -> 'timestamp'
            , bk.block -> 'blockTransactions' -> 'transactions' -> bx.tx_index -> 'outputs' -> bx.output_index as box
        from core.boxes bx
        join core.blocks bk on bk.height = bx.height
        where bx.box_id = any($1);";
    // Vector variant to use as query input
    let box_ids_vec: Vec<&BoxID> = box_ids.iter().collect();
    let rows = pgtx.query(qry, &[&box_ids_vec]).await.unwrap();
    for row in rows {
        let utxo = UTxO {
            height: row.get(0),
            timestamp: serde_json::from_value(row.get(1)).unwrap(),
            output: serde_json::from_value(row.get(2)).unwrap(),
        };
        if !map.contains_key(&utxo.output.box_id) {
            map.insert(utxo.output.box_id.clone(), utxo);
        }
    }
    tracing::debug!("mapped {} box(es)", map.len());
    map
}

pub(super) async fn get_genesis_boxes(pgtx: &Transaction<'_>) -> Vec<node::models::Output> {
    tracing::debug!("retrieving genesis boxes");
    let qry = "
        select json_array_elements(block -> 'blockTransactions' -> 'transactions' -> 0 -> 'outputs') as boxes
        from core.blocks
        where height = 0;";
    let rows = pgtx.query(qry, &[]).await.unwrap();
    rows.iter()
        .map(|r| serde_json::from_value(r.get(0)).unwrap())
        .collect()
}

/// Delete boxes created at `height`
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    pgtx.query("delete from core.boxes where height = $1;", &[&height])
        .await
        .unwrap();
}

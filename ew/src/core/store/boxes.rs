use std::collections::HashMap;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::AddressID;
use crate::core::types::AddressType;
use crate::core::types::Asset;
use crate::core::types::BoxData;
use crate::core::types::BoxID;
use crate::core::types::Digest32;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Registers;

/// A record from the `core.boxes` table.
pub struct BoxRecord<'a> {
    pub box_id: &'a Digest32,
    pub height: Height,
    pub creation_height: Height,
    pub address_id: AddressID,
    pub address_type: AddressType,
    pub value: NanoERG,
    pub size: i32,
    pub assets: Option<Vec<Asset>>,
    pub registers: &'a serde_json::Value,
}

pub(super) async fn insert_many<'a>(pgtx: &Transaction<'_>, records: &Vec<BoxRecord<'a>>) {
    let sql = "insert into core.boxes (
        box_id,
        height,
        creation_height,
        address_id,
        address_type,
        value,
        size,
        assets,
        registers
    ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9);";
    let stmt = pgtx.prepare(sql).await.unwrap();
    for r in records {
        pgtx.execute(
            &stmt,
            &[
                &r.box_id,
                &r.height,
                &r.creation_height,
                &r.address_id,
                &r.address_type,
                &r.value,
                &r.size,
                &r.assets,
                &r.registers,
            ],
        )
        .await
        .unwrap();
    }
}

/// Maps `box_ids` to corresponding BoxData.
pub(super) async fn map_boxes(
    pgtx: &Transaction<'_>,
    box_ids: Vec<&BoxID>,
) -> HashMap<BoxID, BoxData> {
    // tracing::debug!("mapping boxes");
    let mut map: HashMap<BoxID, BoxData> = HashMap::new();
    let qry = "
        select b.box_id
            , b.creation_height
            , b.address_id
            , b.address_type
            , b.value
            , b.size
            , b.assets
            , b.registers
            , h.timestamp
        from core.boxes b
        join core.headers h on h.height = b.height
        where b.box_id = any($1);";

    let rows = pgtx.query(qry, &[&box_ids]).await.unwrap();
    for row in rows {
        let box_data = BoxData {
            box_id: row.get(0),
            creation_height: row.get(1),
            address_id: row.get(2),
            address_type: row.get(3),
            value: row.get(4),
            size: row.get(5),
            assets: row.get::<usize, Option<Vec<Asset>>>(6).unwrap_or(vec![]),
            additional_registers: Registers::new(row.get(7)),
            output_timestamp: row.get(8),
        };
        if !map.contains_key(&box_data.box_id) {
            map.insert(box_data.box_id.clone(), box_data);
        }
    }
    // tracing::debug!("mapped {} box(es)", map.len());
    map
}

/// Retrieves collection of BoxData representing genesis boxes.
pub(super) async fn get_genesis_boxes(client: &Client) -> Vec<BoxData> {
    tracing::trace!("retrieving genesis boxes");
    let qry = "
        select b.box_id
            , b.creation_height
            , b.address_id
            , b.address_type
            , b.value
            , b.size
            , b.assets
            , b.registers
            , h.timestamp
        from core.boxes b
        join core.headers h on h.height = b.height
        where h.height = 0;";
    let rows = client.query(qry, &[]).await.unwrap();
    rows.iter()
        .map(|r| BoxData {
            box_id: r.get(0),
            creation_height: r.get(1),
            address_id: r.get(2),
            address_type: r.get(3),
            value: r.get(4),
            size: r.get(5),
            assets: r.get::<usize, Option<Vec<Asset>>>(6).unwrap_or(vec![]),
            additional_registers: Registers::new(r.get(7)),
            output_timestamp: r.get(8),
        })
        .collect()
}

/// Delete boxes created at `height`
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    pgtx.query("delete from core.boxes where height = $1;", &[&height])
        .await
        .unwrap();
}

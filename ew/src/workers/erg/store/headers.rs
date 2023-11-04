use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::MiniHeader;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::framework::Stamp;

pub async fn get_last(client: &Client) -> Option<MiniHeader> {
    let qry = "
        select height
            , timestamp
            , id
        from erg.headers
        order by height desc
        limit 1;";
    client
        .query_opt(qry, &[])
        .await
        .unwrap()
        .map(|row| MiniHeader {
            height: row.get(0),
            timestamp: row.get(1),
            id: row.get(2),
        })
}

pub async fn insert(pgtx: &Transaction<'_>, header: &MiniHeader) {
    let stmt = "
        insert into erg.headers (height, timestamp, id)
        values ($1, $2, $3);";
    pgtx.execute(stmt, &[&header.height, &header.timestamp, &header.id])
        .await
        .unwrap();
}

pub async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    pgtx.execute("delete from erg.headers where height = $1;", &[&height])
        .await
        .unwrap();
}

/// Returns `true` if core.headers has a record for given `head`.
pub async fn exists(client: &Client, head: &Head) -> bool {
    let sql = "
    select exists (
        select height
            , id
        from erg.headers
        where height = $1 and id = $2
    );";
    client
        .query_one(sql, &[&head.height, &head.header_id])
        .await
        .unwrap()
        .get(0)
}

/// Get id and parent id for given `height`.
///
/// Not for special cases (initial/genesis).
pub async fn get_stamp_at(client: &Client, height: Height) -> Stamp {
    assert!(height > 0);
    let sql = "
        select height
            , id
        from erg.headers
        where height = $1
            or height = $1 - 1
        order by 1 desc
    );";
    let rows = client.query(sql, &[&height]).await.unwrap();
    Stamp {
        height,
        header_id: rows[0].get(1),
        parent_id: rows[1].get(1),
    }
}

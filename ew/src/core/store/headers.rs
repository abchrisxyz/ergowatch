use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::Header;
use crate::core::types::Height;

/// Retrieve head from latest main chain header.
pub(super) async fn get_last_main(client: &Client) -> Option<Header> {
    tracing::trace!("get_last");
    let qry = "
        select height
            , timestamp
            , header_id
            , parent_id
        from core.headers
        where main_chain is True
        order by 1 desc
        limit 1;
    ";
    client.query_opt(qry, &[]).await.unwrap().map(|row| Header {
        height: row.get(0),
        timestamp: row.get(1),
        header_id: row.get(2),
        parent_id: row.get(3),
    })
}

/// Insert new main chain header
pub async fn insert_main(pgtx: &Transaction<'_>, header: &Header) {
    tracing::trace!("insert {header:?}");
    let stmt = "
        insert into core.headers (height, timestamp, header_id, parent_id, main_chain)
        values ($1, $2, $3, $4, True);";
    pgtx.execute(
        stmt,
        &[
            &header.height,
            &header.timestamp,
            &header.header_id,
            &header.parent_id,
        ],
    )
    .await
    .unwrap();
}

/// Delete main chain header at `height`
pub async fn delete_main_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_main_at {height}");
    pgtx.execute(
        "delete from core.headers where height = $1 and main_chain;",
        &[&height],
    )
    .await
    .unwrap();
}

/// Returns `true` if core.headers has a record for given `header` on main chain.
pub async fn exists_and_is_main_chain(client: &Client, header: &Header) -> bool {
    tracing::trace!("exists_and_is_main_chain {header:?}");
    let sql = "
    select exists (
        select height
            , id
        from core.headers
        where height = $1 and id = $2 and main_chain
    );";
    client
        .query_one(sql, &[&header.height, &header.header_id])
        .await
        .unwrap()
        .get(0)
}

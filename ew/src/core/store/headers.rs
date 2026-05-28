use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::Header;

/// Retrieve head from latest main chain header.
pub(super) async fn get_last_main(client: &Client) -> Option<Header> {
    tracing::trace!("get_last");
    let qry = "
        select height
            , timestamp
            , header_id
            , parent_id
        from core.headers
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
        insert into core.headers (height, timestamp, header_id, parent_id)
        values ($1, $2, $3, $4);";
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

/// Pop header from main chain headers and add it to rolled back headers table.
pub async fn roll_back_main_chain_header(pgtx: &Transaction<'_>, header: &Header) {
    // This is all done in a single tx to prevent rolled back header from being
    // "lost" between removal and insertion.
    // Could also implement as two separate calls and rely on caller to call insert
    // before delete, but this is cleaner.
    tracing::trace!("deleting main chain header {}", &header.header_id);
    pgtx.execute(
        "delete from core.headers where height = $1 and header_id = $2;",
        &[&header.height, &header.header_id],
    )
    .await
    .unwrap();

    tracing::trace!("adding header to rolled back headers {}", &header.header_id);
    let stmt = "
        insert into core.rolled_back_headers (height, timestamp, header_id, parent_id)
        values ($1, $2, $3, $4);";
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

/// Returns `true` if core.headers has a record for given `header` on main chain.
pub async fn exists_and_is_main_chain(client: &Client, header: &Header) -> bool {
    tracing::trace!("exists_and_is_main_chain {header:?}");
    let sql = "
    select exists (
        select height
            , header_id
        from core.headers
        where height = $1 and header_id = $2
    );";
    client
        .query_one(sql, &[&header.height, &header.header_id])
        .await
        .unwrap()
        .get(0)
}

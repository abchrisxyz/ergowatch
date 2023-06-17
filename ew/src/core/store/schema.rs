use tokio_postgres::Client;
use tracing::debug;
use tracing::info;

struct Revision {
    pub major: i32,
    pub minor: i32,
}

pub(super) async fn init(client: &mut Client) {
    if !schema_exists(client).await {
        load_schema(client).await;
    }
    let rev = schema_revision(client).await;
    //TODO apply migrations if needed
}

async fn schema_revision(client: &Client) -> Option<Revision> {
    debug!("Reading current revision");
    let qry = "select rev_major, rev_minor from core._meta;";
    match client.query_one(qry, &[]).await {
        Ok(row) => Some(Revision {
            major: row.get(0),
            minor: row.get(1),
        }),
        Err(err) => panic!("{:?}", err),
    }
}

async fn schema_exists(client: &Client) -> bool {
    debug!("Checking for existing schema");
    let qry = "
        select exists(
            select schema_name
            from information_schema.schemata
            where schema_name = 'core'
        );";
    client.query_one(qry, &[]).await.unwrap().get(0)
}

async fn load_schema(client: &mut Client) {
    debug!("Loading schema");
    let sql = include_str!("schema.sql");
    let tx = client.transaction().await.unwrap();
    tx.batch_execute(sql).await.unwrap();
    tx.commit().await.unwrap();
}

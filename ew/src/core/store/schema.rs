use tokio_postgres::Client;

pub struct Schema {
    name: String,
    sql: &'static str,
}

struct Revision {
    pub major: i32,
    pub minor: i32,
}

impl Schema {
    pub fn new(name: &str, sql: &'static str) -> Self {
        Self {
            name: name.to_owned(),
            sql: sql,
        }
    }

    pub async fn init(&self, client: &mut Client) {
        if !self.schema_exists(client).await {
            self.load_schema(client).await;
        }
        let rev = self.schema_revision(client).await;
        if rev.major == 1 && rev.minor == 0 {
            migrations::mig1_1(client).await;
        }
        if rev.major > 1 || rev.minor > 1 {
            todo!("apply miggrations")
        }
    }

    async fn schema_revision(&self, client: &Client) -> Revision {
        tracing::debug!("reading current revision");
        let qry = format!("select rev_major, rev_minor from {}._rev;", self.name);
        match client.query_one(&qry, &[]).await {
            Ok(row) => Revision {
                major: row.get(0),
                minor: row.get(1),
            },
            Err(err) => panic!("{:?}", err),
        }
    }

    async fn schema_exists(&self, client: &Client) -> bool {
        tracing::debug!("checking for existing schema");
        let qry = "
        select exists(
            select schema_name
            from information_schema.schemata
            where schema_name = $1
        );";
        client.query_one(qry, &[&self.name]).await.unwrap().get(0)
    }

    async fn load_schema(&self, client: &mut Client) {
        tracing::debug!("loading schema");
        let tx = client.transaction().await.unwrap();
        tx.batch_execute(self.sql).await.unwrap();
        tx.commit().await.unwrap();
    }
}

mod migrations {
    use super::Client;

    pub(super) async fn mig1_1(client: &mut Client) {
        tracing::info!("migrating core shema to revision 1.1");

        // First, check all headers in core.headers are main_chain.
        // This is pretty much guaranteed since height is a primary key.
        // But to be sure, in case the table was modified outside of ew,
        // check and abort if needed.
        let qry = "
            select count(*)
            from core.headers
            where not main_chain;
        ";
        let not_main_count: i64 = client.query_one(qry, &[]).await.unwrap().get(0);
        if not_main_count > 0 {
            tracing::error!("Found side chain headers in core.headers. This is not expected for core schema revision 1.0.");
            tracing::error!("Aborting migration. A full resync of ErgoWatch is needed.");
            panic!("Cannot proceeed with migration");
        }

        // Begin migration tx
        let tx = client.transaction().await.unwrap();
        tracing::info!("starting migration transaction");

        // Drop main chain column
        tracing::debug!("dropping main_chain column from core.headers");
        tx.execute("alter table core.headers drop column main_chain", &[])
            .await
            .unwrap();

        // Index header id
        tracing::debug!("create index on header ids");
        tx.execute("create index on core.headers(header_id)", &[])
            .await
            .unwrap();

        // New table for rolled back blocks
        tracing::debug!("create new table for rolled back headers");
        tx.execute(
            "
            create table core.rolled_back_headers (
                height integer not null,
                timestamp bigint not null,
                header_id text primary key,
                parent_id text not null
            );",
            &[],
        )
        .await
        .unwrap();

        // Bump revision
        tracing::debug!("bumping core schema revision to 1.1");
        tx.execute("update core._rev set rev_minor = 1", &[])
            .await
            .unwrap();

        tx.commit().await.unwrap();
        tracing::info!("core shema migrated to revision 1.1");
    }
}

use ew::config::PostgresConfig;
use ew::framework::store::Revision;
use tokio_postgres::Client;
use tokio_postgres::NoTls;

use ew::core::types::Header;

pub struct TestDB {
    pub pgconf: PostgresConfig,
    pub client: Client,
}

impl TestDB {
    /// Create new blank test db with given `db_name`.
    pub async fn new(db_name: &str) -> Self {
        tracing::info!("Preparing test db: {}", db_name);

        // Connection string to main test db - see docker-compose-test.yml
        let pg_uri: &str = "postgresql://test:test@localhost:5433/test_db";
        let (client, connection) = tokio_postgres::connect(pg_uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Fresh empty db
        let stmt = format!("drop database if exists {db_name};");
        client.execute(&stmt, &[]).await.unwrap();
        let stmt = format!("create database {db_name};");
        client.execute(&stmt, &[]).await.unwrap();

        // Connection string to new db
        let uri = format!("postgresql://test:test@localhost:5433/{db_name}");

        // Prepare a client for the new db
        let (client, connection) = tokio_postgres::connect(&uri, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Self {
            client,
            pgconf: PostgresConfig::new(&uri),
        }
    }

    /// Initialize core schema.
    pub async fn init_core(&self) {
        self.client
            .batch_execute(include_str!("../../src/core/store/schema.sql"))
            .await
            .unwrap();
    }

    #[allow(dead_code)] // not used by all tests
    /// Initialize ew schema.
    pub async fn init_ew(&self) {
        self.client
            .batch_execute(include_str!("../../src/framework/ew.sql"))
            .await
            .unwrap();
    }

    #[allow(dead_code)] // not used by all tests
    /// Initialize given schema.
    pub async fn init_schema(&self, sql: &str) {
        self.client.batch_execute(sql).await.unwrap();
    }

    /// Insert main chain header into core.headers.
    ///
    /// Needed for rollbacks.
    pub async fn insert_core_header(&self, header: &Header) {
        tracing::trace!("insert {header:?}");
        let stmt = "
        insert into core.headers (height, timestamp, header_id, parent_id, main_chain)
        values ($1, $2, $3, $4, True);";
        self.client
            .execute(
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

    pub async fn get_revision(&self, schema_name: &str, worker_id: &str) -> Option<Revision> {
        let sql =
            "select major, minor from ew.revisions where schema_name = $1 and worker_id = $2;";
        self.client
            .query_opt(sql, &[&schema_name, &worker_id])
            .await
            .unwrap()
            .map(|row| Revision::new(row.get(0), row.get(1)))
    }

    /// Set revision for given schema.
    /// Assumes no revision is set already.
    pub async fn set_revision(&self, schema_name: &str, worker_id: &str, revision: &Revision) {
        let sql = "
            insert into ew.revisions (schema_name, worker_id, major, minor)
            values ($1, $2, $3, $4)";
        self.client
            .execute(
                sql,
                &[&schema_name, &worker_id, &revision.major, &revision.minor],
            )
            .await
            .unwrap();
    }
}

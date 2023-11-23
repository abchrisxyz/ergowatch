use ew::config::PostgresConfig;
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

    pub async fn init_core(&self) {
        self.client
            .batch_execute(include_str!("../../src/core/store/schema.sql"))
            .await
            .unwrap();
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
}

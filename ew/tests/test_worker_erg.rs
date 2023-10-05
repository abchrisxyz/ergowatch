use tokio_postgres::NoTls;

use ew::config::PostgresConfig;
// use ew::core::types::AddressID;
use ew::core::types::Block;
// use ew::core::types::BoxData;
use ew::core::types::CoreData;
// use ew::core::types::Timestamp;
// use ew::core::types::Transaction;
use ew::workers::erg::ErgWorkFlow;
use ew::workers::Workflow;

/// Prepare a test db and return corresponfing config.
async fn prep_db(db_name: &str) -> PostgresConfig {
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
    let stmt = format!("drop database if exists {};", db_name);
    client.execute(&stmt, &[]).await.unwrap();
    let stmt = format!("create database {};", db_name);
    client.execute(&stmt, &[]).await.unwrap();

    // Connection string to new db
    let uri = format!("postgresql://test:test@localhost:5433/{}", db_name);
    PostgresConfig::new(&uri)
}

#[tokio::test]
async fn test_empty_block_pre_launch() {
    let pgconf = prep_db("erg_empty_block_pre").await;
    let block = Block::dummy().height(100);
    let data = CoreData { block };
    let mut workflow = ErgWorkFlow::new(&pgconf).await;
    workflow.include_block(&data).await;
}

#[tokio::test]
async fn test_empty_block_post_launch() {
    let pgconf = prep_db("sigmausd_empty_block_post").await;
    let block = Block::dummy().height(100);
    let mut workflow = ErgWorkFlow::new(&pgconf).await;
    workflow.include_block(&CoreData { block }).await;
}

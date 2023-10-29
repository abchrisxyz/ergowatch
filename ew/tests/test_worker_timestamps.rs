use tokio_postgres::NoTls;

use ew::config::PostgresConfig;
use ew::constants::GENESIS_TIMESTAMP;
use ew::core::types::Block;
use ew::core::types::BoxData;
use ew::core::types::CoreData;
use ew::workers::timestamps::TimestampsWorkFlow;
use ew::workers::Workflow;

pub fn set_tracing_subscriber(set: bool) -> Option<tracing::dispatcher::DefaultGuard> {
    if !set {
        return None;
    }
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    Some(tracing::subscriber::set_default(subscriber))
}

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
async fn test_normal() {
    let _guard = set_tracing_subscriber(false);
    let pgconf = prep_db("timestamps_normal").await;
    // Genesis
    let dummy_genesis_boxes = vec![BoxData::dummy()
        .creation_height(0)
        .timestamp(GENESIS_TIMESTAMP)];
    // Next block
    let block = Block::dummy()
        .height(1)
        .timestamp(GENESIS_TIMESTAMP + 120_000);
    let mut workflow = TimestampsWorkFlow::new(&pgconf).await;
    workflow.include_genesis_boxes(&dummy_genesis_boxes).await;
    workflow.include_block(&CoreData { block }).await;
}

#[tokio::test]
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);
    let pgconf = prep_db("timestamps_rollback").await;

    // Genesis
    let dummy_genesis_boxes = vec![BoxData::dummy()
        .creation_height(0)
        .timestamp(GENESIS_TIMESTAMP)];

    // Block X
    let ts_x = GENESIS_TIMESTAMP + 86_400_000;
    let block_x = Block::dummy().height(1).timestamp(ts_x);

    // Block Y
    let ts_y = ts_x + 120_000;
    let block_y = Block::dummy().height(2).timestamp(ts_y);

    let mut workflow = TimestampsWorkFlow::new(&pgconf).await;
    let height_y = block_y.header.height;
    workflow.include_genesis_boxes(&dummy_genesis_boxes).await;
    workflow.include_block(&CoreData { block: block_x }).await;
    workflow.include_block(&CoreData { block: block_y }).await;

    // Do the rollback
    workflow.roll_back(height_y).await;
}

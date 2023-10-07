use tokio_postgres::NoTls;

use ew::config::PostgresConfig;
use ew::core::types::AddressID;
use ew::core::types::Block;
use ew::core::types::BoxData;
use ew::core::types::CoreData;
use ew::core::types::Timestamp;
use ew::core::types::Transaction;
use ew::workers::erg::ErgWorkFlow;
use ew::workers::Workflow;

const TS_10K: Timestamp = 1563159993440; // timestamp of block 10000

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
async fn test_empty_block_pre_launch() {
    let pgconf = prep_db("erg_empty_block_pre").await;
    let block = Block::dummy().height(100);
    let data = CoreData { block };
    let mut workflow = ErgWorkFlow::new(&pgconf).await;
    workflow.include_block(&data).await;
}

#[tokio::test]
async fn test_empty_block_post_launch() {
    let pgconf = prep_db("erg_empty_block_post").await;
    let block = Block::dummy().height(100);
    let mut workflow = ErgWorkFlow::new(&pgconf).await;
    workflow.include_block(&CoreData { block }).await;
}

#[tokio::test]
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);
    let addr_a: AddressID = 1001;
    let addr_b: AddressID = 1002;
    let addr_c: AddressID = 1003;
    let pgconf = prep_db("erg_rollback").await;

    // Block X
    let block_x = Block::dummy()
        .height(10000)
        .timestamp(TS_10K)
        .add_tx(
            // Create A out of thin air (otherwise A ends up with a negative balance)
            Transaction::dummy()
                .add_output(BoxData::dummy().address_id(addr_a).value(105_000_000_000)),
        )
        .add_tx(
            // A sends 5 to B, creating B
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(addr_a).value(105_000_000_000))
                .add_output(BoxData::dummy().address_id(addr_a).value(100_000_000_000))
                .add_output(BoxData::dummy().address_id(addr_b).value(5_000_000_000)),
        );
    // Block Y
    let block_y = Block::dummy()
        .height(10001)
        .timestamp(TS_10K + 120_000)
        .add_tx(
            // B sends 5 to C, creating C and spending B
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(addr_b).value(5_000_000_000))
                .add_output(BoxData::dummy().address_id(addr_c).value(5_000_000_000)),
        )
        .add_tx(
            // C sends 1 to B, modifying A
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(addr_c).value(1_000_000_000))
                .add_output(BoxData::dummy().address_id(addr_a).value(1_000_000_000)),
        );
    let mut workflow = ErgWorkFlow::new(&pgconf).await;
    let height_y = block_y.header.height;
    workflow.include_block(&CoreData { block: block_x }).await;
    workflow.include_block(&CoreData { block: block_y }).await;
    workflow.roll_back(height_y).await;
}

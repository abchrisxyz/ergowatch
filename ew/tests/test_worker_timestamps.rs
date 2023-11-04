use tokio_postgres::Client;
use tokio_postgres::NoTls;

use ew::config::PostgresConfig;
use ew::constants::GENESIS_TIMESTAMP;
use ew::core::types::Block;
use ew::core::types::BoxData;
use ew::core::types::CoreData;
use ew::framework::Workflow;
use ew::workers::timestamps::TimestampsWorkFlow;

pub fn set_tracing_subscriber(set: bool) -> Option<tracing::dispatcher::DefaultGuard> {
    if !set {
        return None;
    }
    let subscriber = tracing_subscriber::fmt()
        .compact()
        // .with_max_level(tracing::Level::TRACE)
        .with_env_filter("ew=trace")
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

    // Prepare a db client we'll use to inspect the db
    let (client, connection) = tokio_postgres::connect(&pgconf.connection_uri, NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Genesis
    let dummy_genesis_boxes = vec![BoxData::dummy()
        .creation_height(0)
        .timestamp(GENESIS_TIMESTAMP)];
    let genesis_block = Block::from_genesis_boxes(dummy_genesis_boxes);

    // Next block
    let block = Block::dummy()
        .height(1)
        .timestamp(GENESIS_TIMESTAMP + 120_000);
    let mut workflow = TimestampsWorkFlow::new(&pgconf).await;

    // Process blocks
    workflow
        .include_block(&CoreData {
            block: genesis_block,
        })
        .await;
    workflow.include_block(&CoreData { block }).await;

    // Check db
    let hourly = get_hourly_timestamps(&client).await;
    assert_eq!(hourly.len(), 2);
    assert_eq!(hourly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(hourly[1], (1, GENESIS_TIMESTAMP + 120_000));

    let daily = get_daily_timestamps(&client).await;
    assert_eq!(daily.len(), 2);
    assert_eq!(daily[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(daily[1], (1, GENESIS_TIMESTAMP + 120_000));

    let weekly = get_weekly_timestamps(&client).await;
    assert_eq!(weekly.len(), 2);
    assert_eq!(weekly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(weekly[1], (1, GENESIS_TIMESTAMP + 120_000));
}

#[tokio::test]
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);
    let pgconf = prep_db("timestamps_rollback").await;

    // Prepare a db client we'll use to inspect the db
    let (client, connection) = tokio_postgres::connect(&pgconf.connection_uri, NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Genesis
    let dummy_genesis_boxes = vec![BoxData::dummy()
        .creation_height(0)
        .timestamp(GENESIS_TIMESTAMP)];
    let genesis_block = Block::from_genesis_boxes(dummy_genesis_boxes);

    // Block X
    let ts_x = GENESIS_TIMESTAMP + 120_000;
    let block_x = Block::dummy().height(1).timestamp(ts_x);

    // Block Y
    let ts_y = ts_x + 86_400_000;
    let block_y = Block::dummy().height(2).timestamp(ts_y);

    let mut workflow = TimestampsWorkFlow::new(&pgconf).await;
    let height_y = block_y.header.height;

    // Process blocks
    workflow
        .include_block(&CoreData {
            block: genesis_block,
        })
        .await;
    workflow.include_block(&CoreData { block: block_x }).await;
    workflow.include_block(&CoreData { block: block_y }).await;

    // Check db
    let hourly = get_hourly_timestamps(&client).await;
    assert_eq!(hourly.len(), 26);
    assert_eq!(hourly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(hourly[1], (1, GENESIS_TIMESTAMP + 3_600_000));
    assert_eq!(hourly[25], (2, ts_y));

    let daily = get_daily_timestamps(&client).await;
    assert_eq!(daily.len(), 3);
    assert_eq!(daily[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(daily[1], (1, GENESIS_TIMESTAMP + 46_800_000));
    assert_eq!(daily[2], (2, ts_y));

    let weekly = get_weekly_timestamps(&client).await;
    assert_eq!(weekly.len(), 2);
    assert_eq!(weekly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(weekly[1], (2, ts_y));

    // Do the rollback
    workflow.roll_back(height_y).await;

    // Recheck db
    let hourly = get_hourly_timestamps(&client).await;
    assert_eq!(hourly.len(), 2);
    assert_eq!(hourly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(hourly[1], (1, ts_x));

    let daily = get_daily_timestamps(&client).await;
    assert_eq!(daily.len(), 2);
    assert_eq!(daily[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(daily[1], (1, ts_x));

    let weekly = get_weekly_timestamps(&client).await;
    assert_eq!(weekly.len(), 2);
    assert_eq!(weekly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(weekly[1], (1, ts_x));
}

async fn get_hourly_timestamps(client: &Client) -> Vec<(i32, i64)> {
    client
        .query(
            "select height
                , timestamp
            from timestamps.hourly
            order by height;",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i64>(1)))
        .collect()
}

async fn get_daily_timestamps(client: &Client) -> Vec<(i32, i64)> {
    client
        .query(
            "select height
                , timestamp
            from timestamps.daily
            order by height;",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i64>(1)))
        .collect()
}

async fn get_weekly_timestamps(client: &Client) -> Vec<(i32, i64)> {
    client
        .query(
            "select height
                , timestamp
            from timestamps.weekly
            order by height;",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<usize, i32>(0), r.get::<usize, i64>(1)))
        .collect()
}

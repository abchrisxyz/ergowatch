mod db_utils;

use db_utils::TestDB;
use ew::constants::GENESIS_TIMESTAMP;
use ew::constants::ZERO_HEADER;
use ew::core::types::Block;
use ew::core::types::BoxData;
use ew::core::types::CoreData;
use ew::core::types::Header;
use ew::framework::Workflow;
use ew::workers::timestamps::TimestampsWorkFlow;

pub fn set_tracing_subscriber(set: bool) -> Option<tracing::dispatcher::DefaultGuard> {
    if !set {
        return None;
    }
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter("ew=trace")
        .finish();
    Some(tracing::subscriber::set_default(subscriber))
}

#[tokio::test]
async fn test_normal() {
    let _guard = set_tracing_subscriber(false);

    // Prepare test db
    let test_db = TestDB::new("timestamps_normal").await;

    // Genesis
    let dummy_genesis_boxes = vec![BoxData::dummy()
        .creation_height(0)
        .timestamp(GENESIS_TIMESTAMP)];
    let genesis_block = Block::from_genesis_boxes(dummy_genesis_boxes);

    // Next block
    let block = Block::dummy()
        .height(1)
        .parent_id(ZERO_HEADER)
        .timestamp(GENESIS_TIMESTAMP + 120_000);

    let mut workflow = TimestampsWorkFlow::new(&test_db.pgconf).await;

    // Process blocks
    workflow
        .include_block(
            &CoreData {
                block: genesis_block,
            }
            .into(),
        )
        .await;
    workflow.include_block(&CoreData { block }.into()).await;

    // Check db
    let hourly = get_hourly_timestamps(&test_db).await;
    assert_eq!(hourly.len(), 2);
    assert_eq!(hourly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(hourly[1], (1, GENESIS_TIMESTAMP + 120_000));

    let daily = get_daily_timestamps(&test_db).await;
    assert_eq!(daily.len(), 2);
    assert_eq!(daily[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(daily[1], (1, GENESIS_TIMESTAMP + 120_000));

    let weekly = get_weekly_timestamps(&test_db).await;
    assert_eq!(weekly.len(), 2);
    assert_eq!(weekly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(weekly[1], (1, GENESIS_TIMESTAMP + 120_000));
}

#[tokio::test]
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);

    // Prepare test db
    let test_db = TestDB::new("timestamps_rollback").await;
    test_db.init_core().await;

    // Genesis
    let dummy_genesis_boxes = vec![BoxData::dummy()
        .creation_height(0)
        .timestamp(GENESIS_TIMESTAMP)];
    let genesis_block = Block::from_genesis_boxes(dummy_genesis_boxes);

    // Block X
    let ts_x = GENESIS_TIMESTAMP + 120_000;
    let block_x = Block::dummy()
        .height(1)
        .parent_id(ZERO_HEADER)
        .timestamp(ts_x);

    // Block Y
    let ts_y = ts_x + 86_400_000;
    let block_y = Block::child_of(&block_x).timestamp(ts_y);

    let mut workflow = TimestampsWorkFlow::new(&test_db.pgconf).await;
    let height_y = block_y.header.height;

    // Register core header for parent of rolled back blocks
    let h = Header::from(&block_x.header);
    test_db.insert_core_header(&h).await;

    // Process blocks
    workflow
        .include_block(
            &CoreData {
                block: genesis_block,
            }
            .into(),
        )
        .await;
    workflow
        .include_block(&CoreData { block: block_x }.into())
        .await;
    workflow
        .include_block(&CoreData { block: block_y }.into())
        .await;

    // Check db
    let hourly = get_hourly_timestamps(&test_db).await;
    assert_eq!(hourly.len(), 26);
    assert_eq!(hourly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(hourly[1], (1, GENESIS_TIMESTAMP + 3_600_000));
    assert_eq!(hourly[25], (2, ts_y));

    let daily = get_daily_timestamps(&test_db).await;
    assert_eq!(daily.len(), 3);
    assert_eq!(daily[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(daily[1], (1, GENESIS_TIMESTAMP + 46_800_000));
    assert_eq!(daily[2], (2, ts_y));

    let weekly = get_weekly_timestamps(&test_db).await;
    assert_eq!(weekly.len(), 2);
    assert_eq!(weekly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(weekly[1], (2, ts_y));

    // Do the rollback
    workflow.roll_back(height_y).await;

    // Recheck db
    let hourly = get_hourly_timestamps(&test_db).await;
    assert_eq!(hourly.len(), 2);
    assert_eq!(hourly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(hourly[1], (1, ts_x));

    let daily = get_daily_timestamps(&test_db).await;
    assert_eq!(daily.len(), 2);
    assert_eq!(daily[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(daily[1], (1, ts_x));

    let weekly = get_weekly_timestamps(&test_db).await;
    assert_eq!(weekly.len(), 2);
    assert_eq!(weekly[0], (0, GENESIS_TIMESTAMP));
    assert_eq!(weekly[1], (1, ts_x));
}

async fn get_hourly_timestamps(test_db: &TestDB) -> Vec<(i32, i64)> {
    test_db
        .client
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

async fn get_daily_timestamps(test_db: &TestDB) -> Vec<(i32, i64)> {
    test_db
        .client
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

async fn get_weekly_timestamps(test_db: &TestDB) -> Vec<(i32, i64)> {
    test_db
        .client
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

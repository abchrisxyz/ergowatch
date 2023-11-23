mod db_utils;

use db_utils::TestDB;

use ew::constants::GENESIS_TIMESTAMP;
use tokio_postgres::Client;

use ew::core::types::AddressID;
use ew::core::types::Block;
use ew::core::types::BoxData;
use ew::core::types::CoreData;
use ew::core::types::Timestamp;
use ew::core::types::Transaction;
use ew::framework::Workflow;
use ew::workers::erg::ErgWorkFlow;

const TS_10K: Timestamp = 1563159993440; // timestamp of block 10000

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
async fn test_empty_blocks() {
    let _guard = set_tracing_subscriber(false);
    let test_db = TestDB::new("erg_empty_blocks").await;

    // Genesis
    let dummy_genesis_boxes = vec![BoxData::dummy()
        .creation_height(0)
        .timestamp(GENESIS_TIMESTAMP)];
    let genesis_block = Block::from_genesis_boxes(dummy_genesis_boxes);

    // Next block
    let block = Block::child_of(&genesis_block).timestamp(GENESIS_TIMESTAMP + 120_000);

    // Process blocks
    let mut workflow = ErgWorkFlow::new(&test_db.pgconf).await;
    workflow
        .include_block(
            &CoreData {
                block: genesis_block,
            }
            .into(),
        )
        .await;
    workflow.include_block(&CoreData { block }.into()).await;
}

#[tokio::test]
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);
    let addr_a: AddressID = 1001;
    let addr_b: AddressID = 1002;
    let addr_c: AddressID = 1003;
    let test_db = TestDB::new("erg_rollback").await;
    test_db.init_core().await;

    // Genesis
    // Keeping it empty this time to simplify test assertions.
    let genesis_block = Block::from_genesis_boxes(vec![]);

    // Block X
    let ts_x = TS_10K;
    let block_x = Block::child_of(&genesis_block)
        .timestamp(ts_x)
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
    let ts_y = ts_x + 120_000;
    let block_y = Block::child_of(&block_x)
        .timestamp(ts_y)
        .add_tx(
            // B sends 5 to C, creating C and spending B
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(addr_b).value(5_000_000_000))
                .add_output(BoxData::dummy().address_id(addr_c).value(5_000_000_000)),
        )
        .add_tx(
            // C sends 1 to A, modifying A
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(addr_c).value(1_000_000_000))
                .add_output(BoxData::dummy().address_id(addr_a).value(1_000_000_000)),
        );

    // Register core header for parent of rolled back blocks
    test_db.insert_core_header(&(&block_x.header).into()).await;

    let mut workflow = ErgWorkFlow::new(&test_db.pgconf).await;
    let height_y = block_y.header.height;
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

    // Check db state before rollback
    let balances = get_balances(&test_db.client).await;
    assert_eq!(balances.len(), 2);
    assert_eq!(balances[0], (addr_a, 101_000_000_000, 1563159994628));
    assert_eq!(balances[1], (addr_c, 4_000_000_000, TS_10K + 120_000));

    // Do the rollback
    workflow.roll_back(height_y).await;

    // Check db state after rollback
    let balances = get_balances(&test_db.client).await;
    assert_eq!(balances.len(), 2);
    assert_eq!(balances[0], (addr_a, 100_000_000_000, TS_10K - 1));
    assert_eq!(balances[1], (addr_b, 5_000_000_000, TS_10K));
}

async fn get_balances(client: &Client) -> Vec<(i64, i64, i64)> {
    client
        .query(
            "select address_id
                , nano
                , mean_age_timestamp
            from erg.balances
            order by address_id;",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get::<usize, i64>(0),
                r.get::<usize, i64>(1),
                r.get::<usize, i64>(2),
            )
        })
        .collect()
}

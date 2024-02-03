mod db_utils;

use db_utils::TestDB;

use ew::constants::GENESIS_TIMESTAMP;
use ew::core::types::AddressID;
use ew::core::types::AssetID;
use ew::core::types::Block;
use ew::core::types::BoxData;
use ew::core::types::CoreData;
use ew::core::types::Height;
use ew::core::types::Timestamp;
use ew::core::types::Transaction;
use ew::core::types::Value;
use ew::framework::EventHandling;
use ew::workers::tokens::TokensWorkFlow;
use tokio_postgres::Client;

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
    let test_db = TestDB::new("tokens_empty_blocks").await;

    // Genesis
    let dummy_genesis_boxes = vec![BoxData::dummy()
        .creation_height(0)
        .timestamp(GENESIS_TIMESTAMP)];
    let genesis_block = Block::from_genesis_boxes(dummy_genesis_boxes);

    // Next block
    let block = Block::child_of(&genesis_block).timestamp(GENESIS_TIMESTAMP + 120_000);

    // Process blocks
    let mut workflow = TokensWorkFlow::new(&test_db.pgconf).await;
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
    let addr_a = AddressID::dummy(1001);
    let addr_b = AddressID::dummy(1002);
    let addr_c = AddressID::dummy(1003);
    let asset_x: AssetID = 11;
    let test_db = TestDB::new("tokens_rollback").await;
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
            Transaction::dummy().add_output(
                BoxData::dummy()
                    .address_id(addr_a)
                    .add_asset(asset_x, 105_000_000_000),
            ),
        )
        .add_tx(
            // A sends 5 to B, creating B
            Transaction::dummy()
                .add_input(
                    BoxData::dummy()
                        .address_id(addr_a)
                        .add_asset(asset_x, 105_000_000_000),
                )
                .add_output(
                    BoxData::dummy()
                        .address_id(addr_a)
                        .add_asset(asset_x, 100_000_000_000),
                )
                .add_output(
                    BoxData::dummy()
                        .address_id(addr_b)
                        .add_asset(asset_x, 5_000_000_000),
                ),
        );

    // Block Y
    let ts_y = ts_x + 120_000;
    let block_y = Block::child_of(&block_x)
        .timestamp(ts_y)
        .add_tx(
            // B sends 5 to C, creating C and spending B
            Transaction::dummy()
                .add_input(
                    BoxData::dummy()
                        .address_id(addr_b)
                        .add_asset(asset_x, 5_000_000_000),
                )
                .add_output(
                    BoxData::dummy()
                        .address_id(addr_c)
                        .add_asset(asset_x, 5_000_000_000),
                ),
        )
        .add_tx(
            // C sends 1 to A, modifying A
            Transaction::dummy()
                .add_input(
                    BoxData::dummy()
                        .address_id(addr_c)
                        .add_asset(asset_x, 1_000_000_000),
                )
                .add_output(
                    BoxData::dummy()
                        .address_id(addr_a)
                        .add_asset(asset_x, 1_000_000_000),
                ),
        );

    // Register core header for parent of rolled back blocks
    test_db.insert_core_header(&(&block_x.header).into()).await;

    let mut workflow = TokensWorkFlow::new(&test_db.pgconf).await;
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
    let diffs = get_diffs(&test_db.client).await;
    assert_eq!(diffs.len(), 7);
    assert_eq!(
        diffs,
        vec![
            (addr_a, asset_x, 1, 0, 105_000_000_000),
            (addr_a, asset_x, 1, 1, -5_000_000_000),
            (addr_b, asset_x, 1, 1, 5_000_000_000),
            (addr_b, asset_x, 2, 0, -5_000_000_000),
            (addr_c, asset_x, 2, 0, 5_000_000_000),
            (addr_a, asset_x, 2, 1, 1_000_000_000),
            (addr_c, asset_x, 2, 1, -1_000_000_000),
        ]
    );

    // Do the rollback
    workflow.roll_back(height_y).await;

    // Check db state after rollback
    let diffs = get_diffs(&test_db.client).await;
    assert_eq!(diffs.len(), 3);
    assert_eq!(
        diffs,
        vec![
            (addr_a, asset_x, 1, 0, 105_000_000_000),
            (addr_a, asset_x, 1, 1, -5_000_000_000),
            (addr_b, asset_x, 1, 1, 5_000_000_000),
        ]
    );
}

async fn get_diffs(client: &Client) -> Vec<(AddressID, AssetID, Height, i16, Value)> {
    client
        .query(
            "select address_id
                , asset_id
                , height
                , tx_idx
                , value
            from tokens.balance_diffs
            order by height, tx_idx, address_id;",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get::<usize, AddressID>(0),
                r.get::<usize, AssetID>(1),
                r.get::<usize, Height>(2),
                r.get::<usize, i16>(3),
                r.get::<usize, Value>(4),
            )
        })
        .collect()
}

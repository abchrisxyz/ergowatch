mod db_utils;

use db_utils::TestDB;

use ew::constants::GENESIS_TIMESTAMP;
use ew::constants::ZERO_HEADER;
use ew::framework::StampedData;
use ew::workers::erg_diffs::types::DiffRecord;
use tokio_postgres::Client;

use ew::core::types::AddressID;
use ew::core::types::Timestamp;
use ew::framework::EventHandling;
use ew::workers::erg::ErgWorkFlow;
use ew::workers::erg_diffs::types::DiffData;

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
    let genesis_data = StampedData {
        height: 0,
        timestamp: GENESIS_TIMESTAMP,
        header_id: ZERO_HEADER.to_owned(),
        parent_id: "".to_owned(),
        data: DiffData {
            diff_records: vec![],
        },
    };

    // Next block
    let data_1 = genesis_data.wrap_as_child(DiffData {
        diff_records: vec![],
    });

    // Process blocks
    let mut workflow = ErgWorkFlow::new(&test_db.pgconf).await;
    workflow.include_block(&genesis_data).await;
    workflow.include_block(&data_1).await;
}

#[tokio::test]
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);
    let addr_a = AddressID::p2pk(1001);
    let addr_b = AddressID::miner(1002);
    let addr_c = AddressID::other(1003);
    let test_db = TestDB::new("erg_rollback").await;
    test_db.init_core().await;

    // Genesis
    let genesis_data = StampedData {
        height: 0,
        timestamp: GENESIS_TIMESTAMP,
        header_id: ZERO_HEADER.to_owned(),
        parent_id: "".to_owned(),
        data: DiffData {
            diff_records: vec![],
        },
    };

    // Block 1
    let data_1 = genesis_data
        .wrap_as_child(DiffData {
            diff_records: vec![
                // Create A out of thin air
                DiffRecord::new(addr_a, 1, 0, 105_000_000_000),
                // A sends 5 to B, creating B
                DiffRecord::new(addr_a, 1, 1, -5_000_000_000),
                DiffRecord::new(addr_b, 1, 1, 5_000_000_000),
            ],
        })
        .timestamp(TS_10K);

    // Block 2
    let data_2 = data_1
        .wrap_as_child(DiffData {
            diff_records: vec![
                // B sends 5 to C, creating C and spending B
                DiffRecord::new(addr_b, 2, 0, -5_000_000_000),
                DiffRecord::new(addr_c, 2, 0, 5_000_000_000),
                // C sends 1 to A, modifying A
                DiffRecord::new(addr_c, 2, 1, -1_000_000_000),
                DiffRecord::new(addr_a, 2, 1, 1_000_000_000),
            ],
        })
        .timestamp(data_1.timestamp + 120_000);

    // Register core header for parent of rolled back blocks
    test_db.insert_core_header(&data_1.get_header()).await;

    let mut workflow = ErgWorkFlow::new(&test_db.pgconf).await;
    workflow.include_block(&genesis_data).await;
    workflow.include_block(&data_1).await;
    workflow.include_block(&data_2).await;

    // Check db state before rollback
    let balances = get_balances(&test_db.client).await;
    assert_eq!(balances.len(), 2);
    assert_eq!(balances[0], (addr_a, 101_000_000_000, 1563159994628));
    assert_eq!(balances[1], (addr_c, 4_000_000_000, TS_10K + 120_000));

    // Do the rollback
    workflow.roll_back(data_2.height).await;

    // Check db state after rollback
    let balances = get_balances(&test_db.client).await;
    assert_eq!(balances.len(), 2);
    assert_eq!(balances[0], (addr_a, 100_000_000_000, TS_10K));
    assert_eq!(balances[1], (addr_b, 5_000_000_000, TS_10K));
}

async fn get_balances(client: &Client) -> Vec<(AddressID, i64, i64)> {
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
                r.get::<usize, AddressID>(0),
                r.get::<usize, i64>(1),
                r.get::<usize, i64>(2),
            )
        })
        .collect()
}

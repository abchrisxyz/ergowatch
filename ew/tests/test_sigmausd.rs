use ew::config::PostgresConfig;

use ew::core::types::Block;
use ew::core::types::CoreData;
use ew::core::types::Output;
use ew::workers::sigmausd::SigmaUSD;
use ew::workers::Workflow;

use tokio_postgres::NoTls;

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

#[test]
#[cfg(tests)]
fn test_dev() {
    // let block = ew::core::types::Output::voila();
    let i = ew::core::types::testing::Output::voila();
    assert_eq!(i, 5);
}

// #[tokio::test]
// async fn test_dummy_block() {
//     let pgconf = prep_db("sigmausd_empty_batch").await;
// let block = Block::dummy();
// let block = ew::core::types::Output::dummy();
// let data = CoreData { block };
// let workflow = SigmaUSD::new(&pgconf).await;
// let batch: Batch = Batch {
//     header: MiniHeader {
//         height: 1_000_000,
//         timestamp: 1683634223508,
//         id: "dummy".to_string(),
//     },
//     events: vec![],
//     history_record: None,
//     daily_ohlc_records: vec![],
//     weekly_ohlc_records: vec![],
//     monthly_ohlc_records: vec![],
//     service_diffs: vec![],
// };
// store.persist(batch).await;
// }

// #[tokio::test]
// async fn test_populated_batch() {
//     let pgconf = prep_db("sigmausd_populated_batch").await;
//     let mut store = Store::new(pgconf).await;
//     let batch: Batch = Batch {
//         header: MiniHeader {
//             height: 1_000_000,
//             timestamp: 1683634223508,
//             id: "dummy".to_string(),
//         },
//         events: vec![Event::BankTx(BankTransaction {
//             index: 25,
//             height: 1_000_000,
//             reserves_diff: 100,
//             circ_sc_diff: 200,
//             circ_rc_diff: 0,
//             box_id: "bank_box_id_x".to_string(),
//             service_fee: 0,
//             service_address_id: None,
//         })],
//         history_record: Some(HistoryRecord {
//             height: 1_000_000,
//             oracle: 123456789,
//             circ_sc: 1_000_100,
//             circ_rc: 1_000_000_000,
//             reserves: 2_000_000,
//             sc_net: 10_000_000_000,
//             rc_net: 20_000_000_000,
//         }),
//         daily_ohlc_records: vec![],
//         weekly_ohlc_records: vec![],
//         monthly_ohlc_records: vec![],
//         service_diffs: vec![],
//     };
//     store.persist(batch).await;
// }

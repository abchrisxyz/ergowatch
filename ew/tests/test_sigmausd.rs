use tokio_postgres::NoTls;

use ew::config::PostgresConfig;
use ew::core::types::AddressID;
use ew::core::types::Block;
use ew::core::types::CoreData;
use ew::core::types::Input;
use ew::core::types::Output;
use ew::core::types::Timestamp;
use ew::core::types::Transaction;
use ew::workers::sigmausd::constants::BANK_NFT;
use ew::workers::sigmausd::constants::CONTRACT_ADDRESS_ID;
use ew::workers::sigmausd::constants::CONTRACT_CREATION_HEIGHT;
use ew::workers::sigmausd::constants::ORACLE_EPOCH_PREP_ADDRESS_ID;
use ew::workers::sigmausd::constants::ORACLE_NFT;
use ew::workers::sigmausd::constants::SC_TOKEN_ID;
use ew::workers::sigmausd::SigmaUSD;
use ew::workers::Workflow;

// Contract was launched 25 MAR 2021.
// Here's a timestamp rounding up to 26 MAR 2021.
const TS_26MAR2021: Timestamp = 1616761700471;
// This one rounds up to 1 APR 2021.
const TS_01APR2021: Timestamp = 1617283540731;

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
    let pgconf = prep_db("sigmausd_empty_block_pre").await;
    let block = Block::dummy().height(100);
    let data = CoreData { block };
    let mut workflow = SigmaUSD::new(&pgconf).await;
    workflow.include_block(&data).await;
}

#[tokio::test]
async fn test_empty_block_post_launch() {
    let pgconf = prep_db("sigmausd_empty_block_post").await;
    let block = Block::dummy().height(CONTRACT_CREATION_HEIGHT + 100);
    let mut workflow = SigmaUSD::new(&pgconf).await;
    workflow.include_block(&CoreData { block }).await;
}

#[tokio::test]
async fn test_no_events() {
    let pgconf = prep_db("sigmausd_no_events").await;
    let block = Block::dummy()
        .height(CONTRACT_CREATION_HEIGHT + 100)
        .timestamp(TS_26MAR2021);
    let mut workflow = SigmaUSD::new(&pgconf).await;
    workflow.include_block(&CoreData { block }).await;
}

#[tokio::test]
async fn test_sc_minting() {
    let pgconf = prep_db("sigmausd_sc_minting").await;
    let user: AddressID = 12345;
    let block = Block::dummy()
        .height(CONTRACT_CREATION_HEIGHT + 100)
        .timestamp(TS_26MAR2021)
        // User mints 200 SigUSD for 100 ERG
        .add_tx(
            Transaction::dummy()
                // Bank input
                .add_input(
                    Input::dummy()
                        .address_id(CONTRACT_ADDRESS_ID)
                        .value(1000_000_000_000)
                        .add_asset(BANK_NFT, 1)
                        .add_asset(SC_TOKEN_ID, 500_00),
                )
                // User input
                .add_input(Input::dummy().address_id(user).value(5000_000_000_000))
                // Bank output
                .add_output(
                    Output::dummy()
                        .address_id(CONTRACT_ADDRESS_ID)
                        .value(1100_000_000_000)
                        .add_asset(BANK_NFT, 1)
                        .add_asset(SC_TOKEN_ID, 300_00),
                )
                // User output
                .add_output(
                    Output::dummy()
                        .address_id(user)
                        .value(4900_000_000_000)
                        .add_asset(SC_TOKEN_ID, 200_00),
                ),
        );
    let mut workflow = SigmaUSD::new(&pgconf).await;
    workflow.include_block(&CoreData { block }).await;
}

#[tokio::test]
async fn test_rollback() {
    let pgconf = prep_db("sigmausd_rollback").await;
    let user: AddressID = 12345;
    let height = CONTRACT_CREATION_HEIGHT + 100;
    let block = Block::dummy()
        .height(height)
        // Timestamp far enough after contract launch to ensure all
        // ohlc's have a new window. Otherwise, the rollback will
        // delere the only weekly/monthy record that exists (the ones
        // defined in schema.sql).
        .timestamp(TS_01APR2021)
        // User mints 200 SigUSD for 100 ERG
        .add_tx(
            Transaction::dummy()
                // Bank input
                .add_input(
                    Input::dummy()
                        .address_id(CONTRACT_ADDRESS_ID)
                        .value(1000_000_000_000)
                        .add_asset(BANK_NFT, 1)
                        .add_asset(SC_TOKEN_ID, 500_00),
                )
                // User input
                .add_input(Input::dummy().address_id(user).value(5000_000_000_000))
                // Bank output
                .add_output(
                    Output::dummy()
                        .address_id(CONTRACT_ADDRESS_ID)
                        .value(1100_000_000_000)
                        .add_asset(BANK_NFT, 1)
                        .add_asset(SC_TOKEN_ID, 300_00),
                )
                // User output
                .add_output(
                    Output::dummy()
                        .address_id(user)
                        .value(4900_000_000_000)
                        .add_asset(SC_TOKEN_ID, 200_00),
                ),
        )
        // Oracle posting
        .add_tx(
            Transaction::dummy().add_input(Input::dummy()).add_output(
                Output::dummy()
                    .address_id(ORACLE_EPOCH_PREP_ADDRESS_ID)
                    .add_asset(ORACLE_NFT, 1)
                    .set_registers(r#"{"R4": "05baafd2a302"}"#),
            ),
        );

    let mut workflow = SigmaUSD::new(&pgconf).await;
    workflow.include_block(&CoreData { block }).await;
    workflow.roll_back(height).await;
}

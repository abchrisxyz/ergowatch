mod db_utils;

use db_utils::TestDB;

use ew::core::types::AddressID;
use ew::core::types::Block;
use ew::core::types::BoxData;
use ew::core::types::CoreData;
use ew::core::types::Timestamp;
use ew::core::types::Transaction;
use ew::framework::Workflow;
use ew::workers::sigmausd::constants::BANK_NFT;
use ew::workers::sigmausd::constants::CONTRACT_ADDRESS_ID;
use ew::workers::sigmausd::constants::CONTRACT_CREATION_HEIGHT;
use ew::workers::sigmausd::constants::ORACLE_EPOCH_PREP_ADDRESS_ID;
use ew::workers::sigmausd::constants::ORACLE_NFT;
use ew::workers::sigmausd::constants::SC_ASSET_ID;
use ew::workers::sigmausd::SigmaUSD;

// Contract was launched 25 MAR 2021.
// Here's a timestamp rounding up to 26 MAR 2021.
const TS_26MAR2021: Timestamp = 1616761700471;
// This one rounds up to 1 APR 2021.
const TS_01APR2021: Timestamp = 1617283540731;

const CONTRACT_CREATION_HEADER_ID: &str =
    "fd35b157811f0950169e0f86b8f7e9ae0f13c49a46848ff40aa8dad26b030fde";

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

#[tokio::test]
async fn test_empty_block_pre_launch() {
    let _guard = set_tracing_subscriber(false);
    let test_db = TestDB::new("sigmausd_empty_block_pre").await;
    let block = Block::dummy().height(100);
    let data = CoreData { block };
    let mut workflow = SigmaUSD::new(&test_db.pgconf).await;
    workflow.include_block(&data.into()).await;
}

#[tokio::test]
async fn test_empty_block_post_launch() {
    let _guard = set_tracing_subscriber(false);
    let test_db = TestDB::new("sigmausd_empty_block_post").await;
    let block = Block::dummy()
        .height(CONTRACT_CREATION_HEIGHT + 1)
        .parent_id(CONTRACT_CREATION_HEADER_ID);
    let mut workflow = SigmaUSD::new(&test_db.pgconf).await;
    workflow.include_block(&CoreData { block }.into()).await;
}

#[tokio::test]
async fn test_no_events() {
    let _guard = set_tracing_subscriber(false);
    let test_db = TestDB::new("sigmausd_no_events").await;
    let block = Block::dummy()
        .height(CONTRACT_CREATION_HEIGHT + 1)
        .parent_id(CONTRACT_CREATION_HEADER_ID)
        .timestamp(TS_26MAR2021);
    let mut workflow = SigmaUSD::new(&test_db.pgconf).await;
    workflow.include_block(&CoreData { block }.into()).await;
}

#[tokio::test]
async fn test_sc_minting() {
    let _guard = set_tracing_subscriber(false);
    let test_db = TestDB::new("sigmausd_sc_minting").await;
    let user = AddressID::dummy(12345);
    let block = Block::dummy()
        .height(CONTRACT_CREATION_HEIGHT + 1)
        .parent_id(CONTRACT_CREATION_HEADER_ID)
        .timestamp(TS_26MAR2021)
        // User mints 200 SigUSD for 100 ERG
        .add_tx(
            Transaction::dummy()
                // Bank input
                .add_input(
                    BoxData::dummy()
                        .address_id(CONTRACT_ADDRESS_ID)
                        .value(1000_000_000_000)
                        .add_asset(BANK_NFT, 1)
                        .add_asset(SC_ASSET_ID, 500_00),
                )
                // User input
                .add_input(BoxData::dummy().address_id(user).value(5000_000_000_000))
                // Bank output
                .add_output(
                    BoxData::dummy()
                        .address_id(CONTRACT_ADDRESS_ID)
                        .value(1100_000_000_000)
                        .add_asset(BANK_NFT, 1)
                        .add_asset(SC_ASSET_ID, 300_00),
                )
                // User output
                .add_output(
                    BoxData::dummy()
                        .address_id(user)
                        .value(4900_000_000_000)
                        .add_asset(SC_ASSET_ID, 200_00),
                ),
        );
    let mut workflow = SigmaUSD::new(&test_db.pgconf).await;
    workflow.include_block(&CoreData { block }.into()).await;
}

#[tokio::test]
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);
    let test_db = TestDB::new("sigmausd_rollback").await;
    test_db.init_core().await;
    let user = AddressID::dummy(12345);
    let service = AddressID::dummy(6789);

    let block1 = Block::dummy()
        .height(CONTRACT_CREATION_HEIGHT + 1)
        .parent_id(CONTRACT_CREATION_HEADER_ID)
        .timestamp(TS_26MAR2021);

    let block2 = Block::child_of(&block1)
        // Timestamp far enough after contract launch to ensure all
        // ohlc's have a new window. Otherwise, the rollback will
        // delete the only weekly/monthy record that exists (the ones
        // defined in schema.sql).
        .timestamp(TS_01APR2021)
        // User mints 200 SigUSD for 100 ERG + 1 ERG service fee.
        .add_tx(
            Transaction::dummy()
                // Bank input
                .add_input(
                    BoxData::dummy()
                        .address_id(CONTRACT_ADDRESS_ID)
                        .value(1000_000_000_000)
                        .add_asset(BANK_NFT, 1)
                        .add_asset(SC_ASSET_ID, 500_00),
                )
                // User input
                .add_input(BoxData::dummy().address_id(user).value(5000_000_000_000))
                // Bank output
                .add_output(
                    BoxData::dummy()
                        .address_id(CONTRACT_ADDRESS_ID)
                        .value(1100_000_000_000)
                        .add_asset(BANK_NFT, 1)
                        .add_asset(SC_ASSET_ID, 300_00),
                )
                // User output
                .add_output(
                    BoxData::dummy()
                        .address_id(user)
                        .value(4899_000_000_000)
                        .add_asset(SC_ASSET_ID, 200_00),
                )
                // Service output
                .add_output(BoxData::dummy().address_id(service).value(1_000_000_000)),
        )
        // Oracle posting
        .add_tx(
            Transaction::dummy().add_input(BoxData::dummy()).add_output(
                BoxData::dummy()
                    .address_id(ORACLE_EPOCH_PREP_ADDRESS_ID)
                    .add_asset(ORACLE_NFT, 1)
                    .set_registers(r#"{"R4": "05baafd2a302"}"#),
            ),
        );

    // Register core header for block 1 to allow worker to restore it
    // after rolling back block 2.
    test_db.insert_core_header(&(&block1.header).into()).await;

    let mut workflow = SigmaUSD::new(&test_db.pgconf).await;
    workflow
        .include_block(&CoreData { block: block1 }.into())
        .await;
    let block2_height = block2.header.height;
    workflow
        .include_block(&CoreData { block: block2 }.into())
        .await;
    workflow.roll_back(block2_height).await;
}

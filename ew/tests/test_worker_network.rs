mod db_utils;

use db_utils::TestDB;
use ew::constants::address_ids::EMISSION;
use ew::constants::GENESIS_TIMESTAMP;
use ew::constants::ZERO_HEADER;
use ew::core::types::AddressID;
use ew::core::types::Block;
use ew::core::types::BoxData;
use ew::core::types::CoreData;
use ew::core::types::Header;
use ew::core::types::Transaction;
use ew::framework::EventHandling;
use ew::workers::network::Network as NetworkWorkflow;

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
async fn test_simple() {
    let _guard = set_tracing_subscriber(false);

    // Prepare test db
    let test_db = TestDB::new("network_normal").await;
    // Init core schema, needed by network workflow during cache loading.
    test_db.init_core().await;

    // Genesis
    let dummy_genesis_boxes = vec![BoxData::dummy()
        .creation_height(0)
        .timestamp(GENESIS_TIMESTAMP)];
    let genesis_block = Block::from_genesis_boxes(dummy_genesis_boxes);

    // Next block
    let block = Block::dummy()
        .height(1)
        .parent_id(ZERO_HEADER)
        .timestamp(GENESIS_TIMESTAMP + 120_000)
        .add_tx(
            Transaction::dummy()
                .add_input(BoxData::dummy().address_id(EMISSION))
                .add_output(BoxData::dummy().address_id(AddressID::miner(500))),
        )
        // A random unknown extension field
        .add_extension_field("e309", "0100");

    let mut workflow = NetworkWorkflow::new(&test_db.pgconf).await;

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
}

#[tokio::test]
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);

    // Prepare test db
    let test_db = TestDB::new("network_rollback").await;
    test_db.init_core().await;

    // Boostrap block
    // Rollback logic assumes there will always be an existing proposal,
    // so we need a scenario around block 1024 to have one inserted.
    let bootstrap_height = 1023;
    let bootstrap_block = Block::dummy().height(bootstrap_height);

    // Block X
    // Includes votes to start a proposal
    let block_x = Block::child_of(&bootstrap_block).votes([4, 0, 0]).add_tx(
        Transaction::dummy()
            .add_input(BoxData::dummy().address_id(EMISSION))
            .add_output(BoxData::dummy().address_id(AddressID::miner(500))),
    );

    // Block Y
    // Includes vote in favor of proposal
    let block_y = Block::child_of(&block_x).votes([4, 0, 0]).add_tx(
        Transaction::dummy()
            .add_input(BoxData::dummy().address_id(EMISSION))
            .add_output(BoxData::dummy().address_id(AddressID::miner(500))),
    );

    // Initialize workflow a first time
    NetworkWorkflow::new(&test_db.pgconf).await;

    // Change the workflow position
    test_db
        .client
        .execute(
            "update ew.headers set height = $1, header_id = $2, parent_id = $3
            where schema_name = 'network' and worker_id = 'network';",
            &[
                &bootstrap_block.header.height,
                &bootstrap_block.header.id,
                &bootstrap_block.header.parent_id,
            ],
        )
        .await
        .unwrap();

    // Re-initialize the workflow to take above change into consideration
    let mut workflow = NetworkWorkflow::new(&test_db.pgconf).await;

    let height_y = block_y.header.height;

    // Register core header for parent of rolled back blocks
    let h = Header::from(&block_x.header);
    test_db.insert_core_header(&h).await;

    // Process blocks
    workflow
        .include_block(&CoreData { block: block_x }.into())
        .await;
    workflow
        .include_block(&CoreData { block: block_y }.into())
        .await;

    // Check db state prior to rollback
    let proposals = get_proposal_tallies(&test_db).await;
    assert_eq!(proposals.len(), 1);
    // Epoch 1 | 1 yes | 0 yes | 0 yes
    assert_eq!(proposals[0], (1, 1, 0, 0));

    // Do the rollback
    workflow.roll_back(height_y).await;

    // Check db state after rollback
    let proposals = get_proposal_tallies(&test_db).await;
    assert_eq!(proposals.len(), 1);
    // Epoch 1 | 0 yes | 0 yes | 0 yes
    assert_eq!(proposals[0], (1, 0, 0, 0));
}

/// Return epoch and tallies from proposals
async fn get_proposal_tallies(test_db: &TestDB) -> Vec<(i32, i16, i16, i16)> {
    test_db
        .client
        .query(
            "select epoch
                , tallies[1]
                , tallies[2]
                , tallies[3]
            from network.proposals
            order by 1;",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get::<usize, i32>(0),
                r.get::<usize, i16>(1),
                r.get::<usize, i16>(2),
                r.get::<usize, i16>(3),
            )
        })
        .collect()
}

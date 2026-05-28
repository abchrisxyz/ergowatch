// cargo test --test '*' -- --test-threads=1
mod common;
mod db_utils;

use common::blocks::TestBlock;
use db_utils::TestDB;

use ew::core::types::AddressID;
use ew::core::types::Block;
use ew::core::types::CoreData;
use ew::core::types::Header;
use ew::core::types::HeaderID;
use ew::core::types::Height;
use ew::framework::Event;
use ew::framework::Source;
use pretty_assertions::assert_eq;
use tokio;

use common::blocks::TestBlock as TB;
use common::node_mockup::TestNode;
use ew::core::tracking::Tracker;
use ew::core::Node;
use ew::monitor::Monitor;
use tokio::sync::mpsc::error::TryRecvError;

fn set_tracing_subscriber(set: bool) -> Option<tracing::dispatcher::DefaultGuard> {
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

/// Gives some time to tracing subscriber
async fn sleep_some(guard: &Option<tracing::subscriber::DefaultGuard>) {
    if guard.is_some() {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

/// Event wrapper to provide testing helper.
struct EventInspector(Event<CoreData>);

impl EventInspector {
    /// Checks that message is an Include action for given block.
    pub fn assert_includes_block(&self, expected_block: TB) {
        assert_eq!(self.action(), "Include");
        assert_eq!(self.height(), expected_block.height());
        let expected_header_id = expected_block.header_id().to_owned();
        assert_eq!(self.header_id(), Some(expected_header_id));
    }

    /// Checks that message is a Rollback action for given height.
    pub fn assert_rolls_back(&self, expected_height: Height) {
        assert_eq!(self.action(), "Rollback");
        assert_eq!(self.height(), expected_height);
    }

    /// Checks that message is genesis.
    pub fn assert_is_genesis(&self) {
        assert_eq!(self.action(), "Include");
        assert_eq!(self.height(), 0);
        assert_eq!(
            self.header_id().expect("genesis header"),
            "0000000000000000000000000000000000000000000000000000000000000000".to_owned()
        );
    }

    /// Return action of tracking message
    fn action(&self) -> &'static str {
        match self.0 {
            Event::Include(_) => "Include",
            Event::Rollback(_) => "Rollback",
        }
    }

    /// Return height of payload
    fn height(&self) -> Height {
        match &self.0 {
            Event::Include(stamped_data) => {
                let block_height = stamped_data.data.block.header.height;
                assert_eq!(block_height, stamped_data.height);
                block_height
            }
            Event::Rollback(h) => *h,
        }
    }

    /// Return header_id of include message payload
    fn header_id(&self) -> Option<HeaderID> {
        match &self.0 {
            Event::Include(stamped_data) => {
                let header_id = stamped_data.data.block.header.id.clone();
                assert_eq!(header_id, stamped_data.header_id);
                Some(header_id)
            }
            Event::Rollback(_) => None,
        }
    }

    /// Return block of include message payload
    fn block(&self) -> Option<&Block> {
        match &self.0 {
            Event::Include(stamped_data) => Some(&stamped_data.data.block),
            Event::Rollback(_) => None,
        }
    }
}

#[tokio::test]
async fn test_straight_chain_single_cursor() {
    let guard = set_tracing_subscriber(false);
    let block_ids = ["1", "2", "3", "4", "5"];

    // Start a fake node to be queried by the tracker
    let mock_node = TestNode::run(&block_ids).await;

    // Prepare empty db
    let test_db = TestDB::new("test_tracker_1").await;

    // Configure tracker
    let node = Node::new("test-node", mock_node.url());
    let monitor = Monitor::new();
    let mut tracker = Tracker::new(node, test_db.pgconf.clone(), monitor.sender()).await;
    let mut rx = tracker.subscribe(Header::initial(), "C1").await;

    // Start tracker
    tokio::spawn(async move {
        tracker.start().await;
        sleep_some(&guard).await;
    });

    // Collect messages
    let mut messages: Vec<EventInspector> = vec![];
    for _ in 0..6 {
        let event = rx.recv().await.unwrap();
        messages.push(EventInspector(event))
    }

    assert_eq!(messages.len(), 6);
    messages[0].assert_is_genesis();
    messages[1].assert_includes_block(TB::from_id("1"));
    messages[2].assert_includes_block(TB::from_id("2"));
    messages[3].assert_includes_block(TB::from_id("3"));
    messages[4].assert_includes_block(TB::from_id("4"));
    messages[5].assert_includes_block(TB::from_id("5"));
}

#[tokio::test]
async fn test_straight_chain_three_cursors() {
    let guard = set_tracing_subscriber(false);
    let block_ids = ["1", "2", "3", "4", "5"];

    // Start a fake node to be queried by the tracker
    let mock_node = TestNode::run(&block_ids).await;

    // Prepare empty db
    let test_db = TestDB::new("test_tracker_2").await;

    // Monitor
    let monitor = Monitor::new();

    // First, run a single cursor tracker to prepare the store.
    {
        // Configure tracker
        let node = Node::new("test-node", mock_node.url());
        let mut tracker = Tracker::new(node, test_db.pgconf.clone(), monitor.sender()).await;
        // Cursor is at genesis
        let mut rx = tracker.subscribe(Header::initial(), "dummy").await;

        // Start tracker
        tokio::spawn(async move {
            tracker.start().await;
        });

        // Collect messages to ensure tracker is done.
        for _ in 0..6 {
            rx.recv().await.unwrap();
        }
    }

    // Now configure a new tracker with 3 cursors, using the same db.
    let node = Node::new("test-node", mock_node.url());
    let mut tracker = Tracker::new(node, test_db.pgconf.clone(), monitor.sender()).await;
    // First cursor is on last block
    let mut rx_a = tracker.subscribe(TB::from_id("5").header(), "A").await;
    // Second cursor starts from scratch
    let mut rx_b = tracker.subscribe(Header::initial(), "B").await;
    // Third cursor is at block 2
    let mut rx_c = tracker.subscribe(TB::from_id("2").header(), "C").await;

    // Start tracker
    tokio::spawn(async move {
        tracker.start().await;
        sleep_some(&guard).await;
    });

    // Collect messages
    let mut messages_b: Vec<EventInspector> = vec![];
    for _ in 0..6 {
        messages_b.push(EventInspector(rx_b.recv().await.unwrap()))
    }
    let mut messages_c: Vec<EventInspector> = vec![];
    for _ in 3..6 {
        messages_c.push(EventInspector(rx_c.recv().await.unwrap()))
    }
    assert_eq!(rx_a.try_recv().err(), Some(TryRecvError::Empty));

    assert_eq!(messages_b.len(), 6);
    messages_b[0].assert_is_genesis();
    messages_b[1].assert_includes_block(TB::from_id("1"));
    messages_b[2].assert_includes_block(TB::from_id("2"));
    messages_b[3].assert_includes_block(TB::from_id("3"));
    messages_b[4].assert_includes_block(TB::from_id("4"));
    messages_b[5].assert_includes_block(TB::from_id("5"));

    assert_eq!(messages_c.len(), 3);
    messages_c[0].assert_includes_block(TB::from_id("3"));
    messages_c[1].assert_includes_block(TB::from_id("4"));
    messages_c[2].assert_includes_block(TB::from_id("5"));
}

#[tokio::test]
#[ignore = "legacy"] // Untestable as head will be capped to current store's head.
async fn test_fork_handling_not_a_child() {
    let guard = set_tracing_subscriber(false);
    let block_ids = ["1", "2", "3", "3bis*", "4", "5"];

    // Start a fake node to be queried by the tracker
    let mock_node = TestNode::run(&block_ids).await;

    // Prepare empty db
    let test_db = TestDB::new("test_tracker_3").await;

    // Configure tracker
    let node = Node::new("test-node", mock_node.url());
    let monitor = Monitor::new();
    let mut tracker = Tracker::new(node, test_db.pgconf.clone(), monitor.sender()).await;
    // Assuming we've included 1, 2 and 3bis so far
    // Next block will be 4, which isn't a child of 3bis
    let mut rx = tracker.subscribe(TB::from_id("3bis").header(), "C1").await;

    // Start tracker
    tokio::spawn(async move {
        tracker.start().await;
        sleep_some(&guard).await;
    });

    // Collect messages
    let mut messages: Vec<EventInspector> = vec![];
    for _ in 0..4 {
        messages.push(EventInspector(rx.recv().await.unwrap()))
    }

    assert_eq!(messages.len(), 4);
    messages[0].assert_rolls_back(3); // roll back 3bis
    messages[1].assert_includes_block(TB::from_id("3"));
    messages[2].assert_includes_block(TB::from_id("4"));
    messages[3].assert_includes_block(TB::from_id("5"));
}

#[tokio::test]
async fn test_fork_handling_same_height() {
    let guard = set_tracing_subscriber(false);

    // First, process chain 1-2-3bis
    let block_ids = ["1", "2", "3bis"];

    // Start a fake node to be queried by the tracker
    let mut mock_node = TestNode::run(&block_ids).await;

    // Prepare empty db
    let test_db = TestDB::new("test_tracker_4").await;

    // Configure tracker
    let monitor = Monitor::new();
    let mut tracker = Tracker::new(
        Node::new("test-node", &mock_node.url()),
        test_db.pgconf.clone(),
        monitor.sender(),
    )
    .await;
    let mut rx = tracker.subscribe(Header::initial(), "C1").await;

    // Start tracker
    tokio::spawn(async move {
        tracker.start().await;
        sleep_some(&guard).await;
    });

    // Collect first batch of messages
    let mut messages: Vec<EventInspector> = vec![];
    for _ in 0..4 {
        messages.push(EventInspector(rx.recv().await.unwrap()))
    }
    assert_eq!(messages.len(), 4);

    // Simulate fork
    let block_ids = ["1", "2", "3bis*", "3", "4", "5"];
    mock_node.restart(&block_ids).await;

    // Wait for new blocks to be processed
    for _ in 0..4 {
        messages.push(EventInspector(rx.recv().await.unwrap()))
    }

    assert_eq!(messages.len(), 8);
    messages[0].assert_is_genesis();
    messages[1].assert_includes_block(TB::from_id("1"));
    messages[2].assert_includes_block(TB::from_id("2"));
    messages[3].assert_includes_block(TB::from_id("3bis"));
    messages[4].assert_rolls_back(3); // rolls back 3bis
    messages[5].assert_includes_block(TB::from_id("3"));
    messages[6].assert_includes_block(TB::from_id("4"));
    messages[7].assert_includes_block(TB::from_id("5"));

    // Check address and asset id's in blocks 3 and 3 bis.
    // Both have an extra output with different new addresses and assets.
    // Because of the rollback, they should all end up with the same
    // address_id and asset_id. This is what we verify here.

    // Retrieving block data from messages
    let block3b = messages[3].block().unwrap();
    let block3 = messages[5].block().unwrap();

    // Both blocks have 3 outputs
    assert_eq!(block3b.transactions[0].outputs.len(), 3);
    assert_eq!(block3.transactions[0].outputs.len(), 3);

    // Check address id of third output (the extra one)
    // So far, we had 3 genesis boxes (including emission contract)
    // and 3 miners (in blocks 1, 2 and 3), so next address id must be 7 (71 with encoded type)
    assert_eq!(block3b.transactions[0].outputs[2].address_id, AddressID(71));
    assert_eq!(block3.transactions[0].outputs[2].address_id, AddressID(71));

    // Check asset id in third output (the extra one)
    // It is the first token ever encountered, so asset id must be 1
    assert_eq!(block3b.transactions[0].outputs[2].assets[0].asset_id, 1);
    assert_eq!(block3.transactions[0].outputs[2].assets[0].asset_id, 1);

    // Check rolled back header was saved to rolled_back_headers table
    let rb = TB::from_id("3bis");
    let qry = "
            select height
                , timestamp
                , header_id
                , parent_id
            from core.rolled_back_headers
            where header_id = $1
            order by height desc
            limit 1;";
    let h = test_db
        .client
        .query_one(qry, &[&rb.header_id()])
        .await
        .map(|row| Header {
            height: row.get(0),
            timestamp: row.get(1),
            header_id: row.get(2),
            parent_id: row.get(3),
        })
        .unwrap();
    assert_eq!(h.height, rb.height());
    assert_eq!(h.header_id, rb.header_id());
}

/// Check migration 1.1 runs fine
/// Based of test_straight_chain_single_cursor but starting
/// a database that has core schema at rev 1.0
#[tokio::test]
async fn test_mig1_1() {
    let _guard = set_tracing_subscriber(false);
    let block_ids = ["1", "2", "3", "4", "5"];

    // Start a fake node to be queried by the tracker
    let mock_node = TestNode::run(&block_ids).await;

    // Setup db with old schema
    let test_db = TestDB::new("test_core_migration_1_1").await;
    test_db
        .init_schema(include_str!("../src/core/store/schema.1.0.sql"))
        .await;
    let rev = test_db.get_core_revision().await;
    assert_eq!(rev.major, 1);
    assert_eq!(rev.minor, 0);

    // Have at least one header in old schema
    let initial_header = Header::initial();
    let block_1_header = TestBlock::from_id("1").header();
    let stmt = "
        insert into core.headers (height, timestamp, header_id, parent_id, main_chain)
        values
            ($1, $2, $3, $4, TRUE),
            ($5, $6, $7, $8, TRUE);
        ";
    test_db
        .client
        .execute(
            stmt,
            &[
                // initial
                &initial_header.height,
                &initial_header.timestamp,
                &initial_header.header_id,
                &initial_header.parent_id,
                // block 1
                &block_1_header.height,
                &block_1_header.timestamp,
                &block_1_header.header_id,
                &block_1_header.parent_id,
            ],
        )
        .await
        .unwrap();

    // Also prefill at least 1 genesis box, otherwise tracker will add them and fail because
    // core.headers is already at header 1 (there's an assertion checking tracker height
    // when it handles genesis boxes).
    let stmt = "
        insert into core.boxes (box_id, height, creation_height, address_id, value, size, assets, registers)
        values (
            'b69575e11c5c43400bfead5976ee0d6245a1168396b2e2a4f384691f275d501c',
            0, 0, 1, 93409132500000000, 123,
            null, '{}'
        );
        ";
    test_db.client.execute(stmt, &[]).await.unwrap();

    // Configure tracker
    let node = Node::new("test-node", mock_node.url());
    let monitor = Monitor::new();
    let _tracker = Tracker::new(node, test_db.pgconf.clone(), monitor.sender()).await;

    // At this point, the tracker will have applied any migrations to its store.
    let rev = test_db.get_core_revision().await;
    assert_eq!(rev.major, 1);
    assert_eq!(rev.minor, 1);
}

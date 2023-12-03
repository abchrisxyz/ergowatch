// cargo test --test '*' -- --test-threads=1
mod common;

use ew::config::PostgresConfig;
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
use tokio_postgres::NoTls;

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

    // Configure tracker
    let node = Node::new("test-node", mock_node.url());
    let monitor = Monitor::new();
    let mut tracker = Tracker::new(node, prep_db("test_tracker_1").await, monitor.sender()).await;
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
    let pgconf = prep_db("test_tracker_2").await;

    // Monitor
    let monitor = Monitor::new();

    // First, run a single cursor tracker to prepare the store.
    {
        // Configure tracker
        let node = Node::new("test-node", mock_node.url());
        let mut tracker = Tracker::new(node, pgconf.clone(), monitor.sender()).await;
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
    let mut tracker = Tracker::new(node, pgconf, monitor.sender()).await;
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

    // Configure tracker
    let node = Node::new("test-node", mock_node.url());
    let monitor = Monitor::new();
    let mut tracker = Tracker::new(node, prep_db("test_tracker_3").await, monitor.sender()).await;
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

    // Configure tracker
    let monitor = Monitor::new();
    let mut tracker = Tracker::new(
        Node::new("test-node", &mock_node.url()),
        prep_db("test_tracker_4").await,
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

    // Chech address and asset id's in blocks 3 and 3 bis.
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
}

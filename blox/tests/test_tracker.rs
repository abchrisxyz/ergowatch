// cargo test --test '*' -- --test-threads=1
mod common;

use pretty_assertions::assert_eq;
use tokio;

use blox::node::Node;
use blox::render::RenderedBlock;
use blox::Tracker;
use blox::TrackingMessage;
use common::blocks::TestBlock as TB;
use common::node_mockup::TestNode;

fn set_tracing_subscriber(set: bool) -> Option<tracing::dispatcher::DefaultGuard> {
    if !set {
        return None;
    }
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::INFO)
        .finish();
    Some(tracing::subscriber::set_default(subscriber))
}

/// Gives some time to tracing subscriber
async fn sleep_some(guard: &Option<tracing::subscriber::DefaultGuard>) {
    if guard.is_some() {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

const I: &str = "Include";
const R: &str = "Rollback";

struct UnpackedTrackingMessage<'a> {
    variant: &'static str,
    block: &'a RenderedBlock,
}

impl<'a> UnpackedTrackingMessage<'a> {
    pub fn new(te: &'a TrackingMessage) -> Self {
        let (v, b) = match te {
            TrackingMessage::Include(block) => (I, block),
            TrackingMessage::Rollback(block) => (R, block),
        };
        Self {
            variant: v,
            block: b,
        }
    }

    /// Returns (variant, height, header-id) tuple
    pub fn vhi(&self) -> (&str, i32, &str) {
        (
            &self.variant,
            self.block.header.height,
            &self.block.header.id,
        )
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
    let mut tracker = Tracker::new(node);
    let mut rx = tracker.add_cursor(
        "C1".to_string(),
        0,
        "0000000000000000000000000000000000000000000000000000000000000000".to_string(),
    );

    // Start tracker
    tokio::spawn(async move {
        tracker.start().await;
        sleep_some(&guard).await;
    });

    // Collect messages
    let mut messages: Vec<TrackingMessage> = vec![];
    for _ in 0..5 {
        messages.push(rx.recv().await.unwrap())
    }

    // Unpack messages for easier inspection
    let utes: Vec<UnpackedTrackingMessage> = messages
        .iter()
        .map(|m| UnpackedTrackingMessage::new(m))
        .collect();

    assert_eq!(utes.len(), 5);
    assert_eq!(utes[0].vhi(), (I, 1, TB::from_id("1").header_id()));
    assert_eq!(utes[1].vhi(), (I, 2, TB::from_id("2").header_id()));
    assert_eq!(utes[2].vhi(), (I, 3, TB::from_id("3").header_id()));
    assert_eq!(utes[3].vhi(), (I, 4, TB::from_id("4").header_id()));
    assert_eq!(utes[4].vhi(), (I, 5, TB::from_id("5").header_id()));
}

#[tokio::test]
async fn test_straight_chain_three_cursors() {
    let guard = set_tracing_subscriber(false);
    let block_ids = ["1", "2", "3", "4", "5"];

    // Start a fake node to be queried by the tracker
    let mock_node = TestNode::run(&block_ids).await;

    // Configure tracker
    let node = Node::new("test-node", mock_node.url());
    let mut tracker = Tracker::new(node);
    // First cursor is on last block
    let _rx_a = tracker.add_cursor("A".to_string(), 5, TB::from_id("5").header_id().to_string());
    // Second cursor is at genesis
    let mut rx_b = tracker.add_cursor("B".to_string(), 0, TB::from_id("1").parent_id().to_owned());
    // Third cursor is at block 2
    let mut rx_c = tracker.add_cursor("C".to_string(), 2, TB::from_id("2").header_id().to_string());

    // Start tracker
    tokio::spawn(async move {
        tracker.start().await;
        sleep_some(&guard).await;
    });

    // Collect messages
    let messages_a: Vec<TrackingMessage> = vec![];
    let mut messages_b: Vec<TrackingMessage> = vec![];
    for _ in 0..5 {
        messages_b.push(rx_b.recv().await.unwrap())
    }
    let mut messages_c: Vec<TrackingMessage> = vec![];
    for _ in 2..5 {
        messages_c.push(rx_c.recv().await.unwrap())
    }

    // Unpack messages for easier inspection
    let utes_a: Vec<UnpackedTrackingMessage> = messages_a
        .iter()
        .map(|m| UnpackedTrackingMessage::new(m))
        .collect();
    let utes_b: Vec<UnpackedTrackingMessage> = messages_b
        .iter()
        .map(|m| UnpackedTrackingMessage::new(m))
        .collect();
    let utes_c: Vec<UnpackedTrackingMessage> = messages_c
        .iter()
        .map(|m| UnpackedTrackingMessage::new(m))
        .collect();

    assert_eq!(utes_a.len(), 0);
    assert_eq!(utes_b.len(), 5);
    assert_eq!(utes_b[0].vhi(), (I, 1, TB::from_id("1").header_id()));
    assert_eq!(utes_b[1].vhi(), (I, 2, TB::from_id("2").header_id()));
    assert_eq!(utes_b[2].vhi(), (I, 3, TB::from_id("3").header_id()));
    assert_eq!(utes_b[3].vhi(), (I, 4, TB::from_id("4").header_id()));
    assert_eq!(utes_b[4].vhi(), (I, 5, TB::from_id("5").header_id()));
    assert_eq!(utes_c.len(), 3);
    assert_eq!(utes_c[0].vhi(), (I, 3, TB::from_id("3").header_id()));
    assert_eq!(utes_c[1].vhi(), (I, 4, TB::from_id("4").header_id()));
    assert_eq!(utes_c[2].vhi(), (I, 5, TB::from_id("5").header_id()));
}

#[tokio::test]
async fn test_fork_handling_not_a_child() {
    let guard = set_tracing_subscriber(false);
    let block_ids = ["1", "2", "3", "3bis*", "4", "5"];

    // Start a fake node to be queried by the tracker
    let mock_node = TestNode::run(&block_ids).await;

    // Configure tracker
    let node = Node::new("test-node", mock_node.url());
    let mut tracker = Tracker::new(node);
    // Assuming we've included 1, 2 and 3bis so far
    // Next block will be 4, which isn't a child of 3bis
    let mut rx = tracker.add_cursor(
        "C1".to_string(),
        3,
        TB::from_id("3bis").header_id().to_owned(),
    );

    // Start tracker
    tokio::spawn(async move {
        tracker.start().await;
        sleep_some(&guard).await;
    });

    // Collect messages
    let mut messages: Vec<TrackingMessage> = vec![];
    for _ in 0..4 {
        messages.push(rx.recv().await.unwrap())
    }
    assert_eq!(messages.len(), 4);

    // Unpack messages for easier inspection
    let utes: Vec<UnpackedTrackingMessage> = messages
        .iter()
        .map(|m| UnpackedTrackingMessage::new(m))
        .collect();

    assert_eq!(utes.len(), 4);
    assert_eq!(utes[0].vhi(), (R, 3, TB::from_id("3bis").header_id()));
    assert_eq!(utes[1].vhi(), (I, 3, TB::from_id("3").header_id()));
    assert_eq!(utes[2].vhi(), (I, 4, TB::from_id("4").header_id()));
    assert_eq!(utes[3].vhi(), (I, 5, TB::from_id("5").header_id()));
}

#[tokio::test]
async fn test_fork_handling_same_height() {
    let guard = set_tracing_subscriber(false);

    // First, process chain 1-2-3bis
    let block_ids = ["1", "2", "3bis"];

    // Start a fake node to be queried by the tracker
    let mut mock_node = TestNode::run(&block_ids).await;

    // Configure tracker
    let mut tracker = Tracker::new(Node::new("test-node", &mock_node.url()));
    let mut rx = tracker.add_cursor("C1".to_string(), 0, TB::from_id("1").parent_id().to_owned());

    // Start tracker
    tokio::spawn(async move {
        tracker.start().await;
        sleep_some(&guard).await;
    });

    // Collect first batch of messages
    let mut messages: Vec<TrackingMessage> = vec![];
    for _ in 0..3 {
        messages.push(rx.recv().await.unwrap())
    }
    assert_eq!(messages.len(), 3);

    // Simulate fork
    let block_ids = ["1", "2", "3bis*", "3", "4", "5"];
    mock_node.restart(&block_ids).await;

    // Wait for new blocks to be processed
    for _ in 0..4 {
        messages.push(rx.recv().await.unwrap())
    }
    assert_eq!(messages.len(), 7);

    // Unpack messages for easier inspection
    let utes: Vec<UnpackedTrackingMessage> = messages
        .iter()
        .map(|m| UnpackedTrackingMessage::new(m))
        .collect();

    assert_eq!(utes.len(), 7);
    assert_eq!(utes[0].vhi(), (I, 1, TB::from_id("1").header_id()));
    assert_eq!(utes[1].vhi(), (I, 2, TB::from_id("2").header_id()));
    assert_eq!(utes[2].vhi(), (I, 3, TB::from_id("3bis").header_id()));
    assert_eq!(utes[3].vhi(), (R, 3, TB::from_id("3bis").header_id()));
    assert_eq!(utes[4].vhi(), (I, 3, TB::from_id("3").header_id()));
    assert_eq!(utes[5].vhi(), (I, 4, TB::from_id("4").header_id()));
    assert_eq!(utes[6].vhi(), (I, 5, TB::from_id("5").header_id()));
}

mod coingecko;
mod db_utils;

use async_trait::async_trait;
use coingecko::APIData;
use coingecko::MockGecko;
use db_utils::TestDB;
use ew::core::types::Block;
use ew::framework::StampedData;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_postgres::Client;

use ew::core::types::CoreData;
use ew::core::types::Header;
use ew::core::types::Height;
use ew::framework::Event;
use ew::framework::Source;
use ew::workers::coingecko::types::BlockRecord;
use ew::workers::coingecko::types::HourlyRecord;
use ew::workers::coingecko::types::ProvisionalBlockRecord;
use ew::workers::coingecko::Worker;

fn now_in_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        * 1000
}

struct MockSource {
    tx: Option<mpsc::Sender<Event<CoreData>>>,
    header: Header,
}

impl MockSource {
    pub fn new() -> Self {
        Self {
            tx: None,
            header: Header {
                height: 1_000_000,
                timestamp: 0,
                header_id: "fake_header_id_form_source_mockup".to_owned(),
                parent_id: "fake_parent_id_form_source_mockup".to_owned(),
            },
        }
    }

    /// Send an include event with dummy data and given height and timestamp.
    pub async fn send_include(&self, block: Block) {
        let data = StampedData {
            height: block.header.height,
            timestamp: block.header.timestamp,
            header_id: block.header.id.clone(),
            parent_id: block.header.parent_id.clone(),
            data: CoreData { block },
        };
        let event = Event::Include(Arc::new(data));
        match self.tx.as_ref().unwrap().send(event).await {
            Err(e) => panic!("{e}"),
            _ => (),
        };
    }

    pub async fn send_rollback(&self, height: Height) {
        let event = Event::Rollback(height);
        match self.tx.as_ref().unwrap().send(event).await {
            Err(e) => panic!("{e}"),
            _ => (),
        };
    }

    pub fn has_no_pending_events(&self) -> bool {
        let tx = self.tx.as_ref().unwrap();
        tx.capacity() == tx.max_capacity()
    }
}

#[async_trait]
impl Source for MockSource {
    type S = CoreData;

    fn header(&self) -> &Header {
        &self.header
    }

    /// Returns true if `head` is part of source's processed main cahin.
    async fn contains_header(&self, _header: &Header) -> bool {
        // Always return true for dummy impl
        true
    }

    async fn subscribe(
        &mut self,
        _header: Header,
        _cursor_name: &str,
    ) -> mpsc::Receiver<Event<Self::S>> {
        assert!(self.tx.is_none());
        let (tx, rx) = mpsc::channel(ew::framework::EVENT_CHANNEL_CAPACITY);
        self.tx.replace(tx);
        rx
    }
}

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
async fn test_rollback() {
    let _guard = set_tracing_subscriber(false);
    let test_db = TestDB::new("coingecko_rollback").await;
    test_db.init_core().await;

    // Prepare a fake coingecko api data (all after 1561978800000 genesis)
    let nowish = now_in_ms();
    let fake_api_data: APIData = vec![
        (1561979001925, 5.0),
        (1561982568142, 6.0),
        (1561986026324, 7.0),
        (1561989725757, 8.0),
        (1561993390008, 6.5),
        (nowish, 9.0),
    ];

    let mut mock_api = MockGecko::new();
    mock_api.serve(fake_api_data).await;

    // Preparee a fake source
    let mut source = MockSource::new();

    // Prepare the worker
    // Using a dummy monitor sender,
    let (mon_tx, mut mon_rx) = mpsc::channel(10);
    let mut worker = Worker::new(
        &test_db.pgconf,
        &mut source,
        mon_tx,
        Some(&mock_api.get_url()),
    )
    .await;

    // Start worker
    tokio::spawn(async move {
        worker.start().await;
    });

    // Send some events to worker
    let block0 = Block::from_genesis_boxes(vec![]);
    let block1 = Block::child_of(&block0);
    let block2 = Block::child_of(&block1);
    let block3 = Block::child_of(&block2);
    let header2 = Header::from(&block2.header);
    source.send_include(block0).await;
    source.send_include(block1).await;
    source.send_include(block2).await;
    // Wait some before sending last block
    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
    source.send_include(block3).await;

    // Wair for all blocks to be processed
    for _ in 0..4 {
        mon_rx.recv().await;
    }

    // Rollback
    // Register core header for parent of rolled back block
    test_db.insert_core_header(&header2).await;

    // Send rollback
    source.send_rollback(3).await;
    // And wait for it to be processed
    mon_rx.recv().await;

    // Check db state
    let hourly_records = get_hourly_records(&test_db.client).await;
    assert_eq!(
        hourly_records,
        vec![
            HourlyRecord::genesis(),
            HourlyRecord::new(1561979001925, 5.0),
            HourlyRecord::new(1561982568142, 6.0),
            HourlyRecord::new(1561986026324, 7.0),
            HourlyRecord::new(1561989725757, 8.0),
            HourlyRecord::new(1561993390008, 6.5),
            // HourlyRecord::new(nowish, 9.0), // not included as too close to now
        ]
    );

    let block_records = get_block_records(&test_db.client).await;
    assert_eq!(
        block_records,
        vec![
            BlockRecord::new(0, 5.581469768257971),
            BlockRecord::new(1, 5.581469768257971),
            BlockRecord::new(2, 5.581469768257971),
        ]
    );

    let provisional_records = get_provisional_records(&test_db.client).await;
    assert!(provisional_records.is_empty());

    // Ensure there are no pending events left
    assert!(source.has_no_pending_events());
    assert!(mon_rx.try_recv().is_err());

    mock_api.stop().await;
}

async fn get_block_records(client: &Client) -> Vec<BlockRecord> {
    let sql = "select height, value from coingecko.ergusd_block order by 1;";
    client
        .query(sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| BlockRecord {
            height: row.get(0),
            usd: row.get(1),
        })
        .collect()
}

async fn get_hourly_records(client: &Client) -> Vec<HourlyRecord> {
    let sql = "select timestamp, value from coingecko.ergusd_hourly order by 1;";
    client
        .query(sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| HourlyRecord {
            timestamp: row.get(0),
            usd: row.get(1),
        })
        .collect()
}

async fn get_provisional_records(client: &Client) -> Vec<ProvisionalBlockRecord> {
    let sql = "select height, timestamp from coingecko.ergusd_provisional_blocks order by 1;";
    client
        .query(sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| ProvisionalBlockRecord {
            height: row.get(0),
            timestamp: row.get(1),
        })
        .collect()
}

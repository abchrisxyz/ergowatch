mod parsing;
pub mod queries;
mod store;
pub mod types;

use async_trait::async_trait;
use tokio::sync::mpsc;

use self::queries::DiffsQuery;
use self::queries::DiffsQueryResponse;
use self::store::QueryStore;
use self::types::DiffData;
use crate::config::PostgresConfig;
use crate::core::types::CoreData;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::BlockRange;
use crate::framework::EventEmission;
use crate::framework::EventHandling;
use crate::framework::QueryHandler;
use crate::framework::QuerySender;
use crate::framework::QueryWrapper;
use crate::framework::SourceWorker;
use crate::framework::StampedData;
use parsing::Parser;
use store::Store;

const WORKER_ID: &'static str = "erg_diffs";

pub type Worker = SourceWorker<ErgDiffsWorkFlow>;

pub struct ErgDiffsWorkFlow {
    parser: Parser,
    store: Store,
}

#[async_trait]
impl EventHandling for ErgDiffsWorkFlow {
    type U = CoreData;
    type D = DiffData;

    async fn new(pgconf: &PostgresConfig) -> Self {
        let store = Store::new(pgconf, &store::SCHEMA).await;
        let parser = Parser::new();
        Self { parser, store }
    }

    async fn include_block(&mut self, data: &StampedData<CoreData>) -> Self::D {
        let stamped_batch = self.parser.extract_batch(data);
        self.store.persist(&stamped_batch).await;

        DiffData::from(stamped_batch.data)
    }

    async fn roll_back(&mut self, height: Height) -> Header {
        self.store.roll_back(height).await;
        self.parser = Parser::new();
        self.store.get_header().clone()
    }

    fn header<'a>(&'a self) -> &'a Header {
        self.store.get_header()
    }
}

#[async_trait]
impl EventEmission for ErgDiffsWorkFlow {
    type S = DiffData;

    /// Returns true if data for `header` has been included.
    async fn contains_header(&self, header: &Header) -> bool {
        // Initial header is always contained but will not be stored,
        // so handle explicitly.
        header.is_initial() || self.store.is_main_chain(header).await
    }

    /// Get data for given height range.
    ///
    /// Used by lagging cursors to retrieve data.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip(self))]
    async fn get_slice(&self, block_range: &BlockRange) -> Vec<StampedData<Self::S>> {
        self.store.get_slice(block_range).await
    }
}

pub struct QueryWorker {
    store: store::QueryStore,
    query_tx: mpsc::Sender<QueryWrapper<DiffsQuery, DiffsQueryResponse>>,
    query_rx: mpsc::Receiver<QueryWrapper<DiffsQuery, DiffsQueryResponse>>,
}

impl QueryWorker {
    pub async fn new(pgconf: &PostgresConfig) -> Self {
        let (query_tx, query_rx) = mpsc::channel(8);

        Self {
            store: QueryStore::new(pgconf).await,
            query_tx,
            query_rx,
        }
    }

    #[tracing::instrument(name = "erg_diffs query handler", skip_all, level=tracing::Level::DEBUG)]
    pub async fn start(&mut self) {
        tracing::debug!("starting");
        loop {
            tracing::debug!("waiting for next query");
            let qw = self.query_rx.recv().await.unwrap();
            let response = self.store.query_balance_diffs(qw.query).await;
            tracing::debug!("sending response");
            qw.response_tx.send(response).unwrap();
        }
    }
}

impl QueryHandler for QueryWorker {
    type Q = DiffsQuery;
    type R = DiffsQueryResponse;

    fn connect(&self) -> QuerySender<Self::Q, Self::R> {
        QuerySender::new(self.query_tx.clone())
    }
}

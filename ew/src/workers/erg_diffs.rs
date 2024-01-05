mod parsing;
pub mod queries;
mod store;
pub mod types;

use async_trait::async_trait;

use self::types::DiffData;
use crate::config::PostgresConfig;
use crate::core::types::CoreData;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::EventEmission;
use crate::framework::EventHandling;
use crate::framework::QueryHandling;
use crate::framework::QueryableSourceWorker;
use crate::framework::StampedData;
use parsing::Parser;
use store::Store;

const WORKER_ID: &'static str = "erg_diffs";

pub type Worker = QueryableSourceWorker<ErgDiffsWorkFlow>;

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

    /// Get data for given `head`.
    ///
    /// Used by lagging cursors to retrieve data.
    async fn get_at(&self, height: Height) -> StampedData<Self::S> {
        self.store.get_at(height).await
    }
}

#[async_trait]
impl QueryHandling for ErgDiffsWorkFlow {
    type Q = queries::DiffsQuery;
    type R = queries::DiffsQueryResponse;

    async fn execute(&self, query: Self::Q) -> Self::R {
        self.store.query_balance_diffs(query).await
    }
}
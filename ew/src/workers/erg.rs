mod parsing;
mod store;
mod types;

use async_trait::async_trait;

use crate::config::PostgresConfig;
use crate::core::types::CoreData;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::SourceWorker;
use crate::framework::Sourceable;
use crate::framework::StampedData;
use crate::framework::Workflow;
use parsing::Parser;
use store::Store;
use types::BalData;
use types::Batch;

const WORKER_ID: &'static str = "erg";

// switch to framework::SourceWorker here
pub type Worker = SourceWorker<ErgWorkFlow>;

pub struct ErgWorkFlow {
    parser: Parser,
    store: Store,
}

#[async_trait]
impl Workflow for ErgWorkFlow {
    type U = CoreData;
    type D = BalData;

    async fn new(pgconf: &PostgresConfig) -> Self {
        let store = Store::new(pgconf, &store::SCHEMA, &WORKER_ID).await;
        let cache = store::load_parser_cache(store.get_client()).await;
        let parser = Parser::new(cache);
        Self { parser, store }
    }

    async fn include_block(&mut self, data: &StampedData<CoreData>) -> Self::D {
        // Get current balances for all addresses within block
        let balances = self
            .store
            .map_balance_records(data.data.block.transacting_addresses())
            .await;
        let stamped_batch = self.parser.extract_batch(data, balances);
        self.store.persist(&stamped_batch).await;

        BalData::from(stamped_batch.data)
    }

    async fn roll_back(&mut self, height: Height) -> Header {
        self.store.roll_back(height).await;
        // Refresh parser cache to reflect rollback
        let cache = store::load_parser_cache(self.store.get_client()).await;
        self.parser = Parser::new(cache);
        self.store.get_header().clone()
    }

    fn header<'a>(&'a self) -> &'a Header {
        self.store.get_header()
    }
}

#[async_trait]
impl Sourceable for ErgWorkFlow {
    type S = BalData;

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

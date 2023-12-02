mod parsing;
mod store;
mod types;

use async_trait::async_trait;

use crate::config::PostgresConfig;
use crate::core::types::CoreData;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::StampedData;
use crate::framework::Worker as SimpleWorker;
use crate::framework::Workflow;
use parsing::Parser;
use store::Store;

const WORKER_ID: &'static str = "timestamps";

pub type Worker = SimpleWorker<TimestampsWorkFlow>;

pub struct TimestampsWorkFlow {
    parser: Parser,
    store: Store,
}

#[async_trait]
impl Workflow for TimestampsWorkFlow {
    type U = CoreData;
    type D = ();

    async fn new(pgconf: &PostgresConfig) -> Self {
        let store = Store::new(pgconf, &store::SCHEMA).await;
        let cache = store::load_parser_cache(store.get_client()).await;
        let parser = Parser::new(cache);
        Self { parser, store }
    }

    async fn include_block(&mut self, data: &StampedData<CoreData>) {
        // Make extract_batch return StampedData<Batch>
        let stamped_batch = self.parser.extract_batch(&data);
        self.store.persist(&stamped_batch).await;
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

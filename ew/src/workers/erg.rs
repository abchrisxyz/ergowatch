mod parsing;
mod store;
mod types;

use async_trait::async_trait;

use crate::config::PostgresConfig;
use crate::core::types::BoxData;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Height;
use parsing::Parser;
use store::Store;
use types::Batch;

pub type Worker = super::Worker<ErgWorkFlow>;

pub struct ErgWorkFlow {
    parser: Parser,
    store: Store,
}

#[async_trait]
impl super::Workflow for ErgWorkFlow {
    async fn new(pgconf: &PostgresConfig) -> Self {
        let store = Store::new(pgconf.clone()).await;
        tracing::debug!("head: {:?}", store.get_head());
        let cache = store.load_parser_cache().await;
        let parser = Parser::new(cache);
        Self { parser, store }
    }

    async fn include_genesis_boxes(&mut self, boxes: &Vec<BoxData>) {
        let batch = self.parser.extract_genesis_batch(boxes);
        self.store.persist(batch).await;
    }

    async fn include_block(&mut self, data: &CoreData) {
        // Get current balances for all addresses within block
        let balances = self
            .store
            .map_balance_records(data.block.transacting_addresses())
            .await;
        let batch = self.parser.extract_batch(data, balances);
        self.store.persist(batch).await;
    }

    async fn roll_back(&mut self, height: Height) {
        self.store.roll_back(height).await;
        // Refresh parser cache to reflect rollback
        let cache = self.store.load_parser_cache().await;
        self.parser = Parser::new(cache);
    }

    fn head<'a>(&'a self) -> &'a Head {
        self.store.get_head()
    }
}

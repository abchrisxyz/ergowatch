mod constants;
mod parsing;
mod store;
mod types;

use crate::config::PostgresConfig;
use crate::core::tracking::Tracker;
use crate::core::types::Head;

use constants::CONTRACT_CREATION_HEIGHT;
use parsing::Parser;
use store::Store;
use types::Batch;

pub type Worker = super::Worker<SigmaUSD>;

pub struct SigmaUSD {
    parser: Parser,
    store: Store,
}

use crate::core::types::CoreData;
use crate::core::types::Height;
use crate::core::types::Output;
use async_trait::async_trait;

#[async_trait]
impl super::Workflow for SigmaUSD {
    async fn new(pgconf: &PostgresConfig) -> Self {
        let store = Store::new(pgconf.clone()).await;
        let cache = store.load_parser_cache().await;
        let parser = Parser::new(cache);
        Self { parser, store }
    }

    async fn include_genesis_boxes(&mut self, _boxes: &Vec<Output>) {
        // Nothing to do here
    }

    async fn include_block(&mut self, data: &CoreData) {
        // Ignore all data until after contract creation
        if data.block.header.height > CONTRACT_CREATION_HEIGHT {
            let batch = self.parser.extract_batch(data);
            self.store.persist(batch).await;
        }
    }

    async fn roll_back(&mut self, height: Height) {
        self.store.roll_back(height).await;
        // Refresh parser cache to reflect rollback
        let cache = self.store.load_parser_cache().await;
        self.parser = Parser::new(cache);
    }

    async fn head(&self) -> Head {
        self.store.get_head().await
    }
}

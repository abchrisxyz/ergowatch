pub mod constants;
mod parsing;
mod store;
mod types;

use async_trait::async_trait;

use crate::config::PostgresConfig;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::framework::Workflow;
use constants::CONTRACT_CREATION_HEIGHT;
use parsing::Parser;
use store::Store;
use types::Batch;

pub type Worker = crate::framework::Worker<SigmaUSD>;

pub struct SigmaUSD {
    parser: Parser,
    store: Store,
}

#[async_trait]
impl Workflow for SigmaUSD {
    type U = CoreData;
    type D = ();

    async fn new(pgconf: &PostgresConfig) -> Self {
        let store = Store::new(pgconf.clone()).await;
        tracing::debug!("head: {:?}", store.get_head());
        let cache = store.load_parser_cache().await;
        let parser = Parser::new(cache);
        Self { parser, store }
    }

    async fn include_block(&mut self, data: &CoreData) {
        // Ignore all data until after contract creation
        if data.block.header.height > CONTRACT_CREATION_HEIGHT {
            let batch = self.parser.extract_batch(data);
            self.store.persist(batch).await;
        }
    }

    async fn roll_back(&mut self, height: Height) -> Head {
        self.store.roll_back(height).await;
        // Refresh parser cache to reflect rollback
        let cache = self.store.load_parser_cache().await;
        self.parser = Parser::new(cache);
        self.store.get_head().clone()
    }

    fn head<'a>(&'a self) -> &'a Head {
        self.store.get_head()
    }
}

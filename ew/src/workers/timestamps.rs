mod parsing;
mod store;
mod types;

use async_trait::async_trait;

use crate::config::PostgresConfig;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::framework::Worker as SimpleWorker;
use crate::framework::Workflow;
use parsing::Parser;
use store::Store;

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
        let store = Store::new(pgconf.clone()).await;
        let cache = store.load_parser_cache().await;
        let parser = Parser::new(cache);
        Self { parser, store }
    }

    async fn include_block(&mut self, data: &CoreData) {
        let batch = self.parser.extract_batch(&data);
        self.store.persist(batch).await;
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

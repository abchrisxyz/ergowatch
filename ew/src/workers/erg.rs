mod parsing;
mod store;
mod types;

use async_trait::async_trait;

use crate::config::PostgresConfig;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::framework::SourceWorker;
use crate::framework::Sourceable;
use crate::framework::StampedData;
use crate::framework::Workflow;
use parsing::Parser;
use store::Store;
use types::BalData;
use types::Batch;

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
        let store = Store::new(pgconf.clone()).await;
        tracing::debug!("head: {:?}", store.get_head());
        let cache = store.load_parser_cache().await;
        let parser = Parser::new(cache);
        Self { parser, store }
    }

    async fn include_block(&mut self, data: &CoreData) -> Self::D {
        // Get current balances for all addresses within block
        let balances = self
            .store
            .map_balance_records(data.block.transacting_addresses())
            .await;
        let batch = self.parser.extract_batch(data, balances);
        self.store.persist(&batch).await;

        BalData::from(batch)
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

#[async_trait]
impl Sourceable for ErgWorkFlow {
    type S = BalData;

    async fn contains_head(&self, head: &Head) -> bool {
        // Initial head is always contained but will not be stored,
        // so hande explicitly.
        head.is_initial() || self.store.contains_head(head).await
    }

    async fn get_at(&self, height: Height) -> StampedData<Self::S> {
        self.store.get_at(height).await
    }
}

mod parsing;
mod store;
pub mod types;

use async_trait::async_trait;

use crate::config::PostgresConfig;
use crate::core::types::CoreData;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::EventHandling;
use crate::framework::LeafWorker;
use crate::framework::StampedData;
use parsing::Parser;
use store::Store;

const WORKER_ID: &'static str = "tokens";

pub type Worker = LeafWorker<TokensWorkFlow>;

pub struct TokensWorkFlow {
    parser: Parser,
    store: Store,
}

#[async_trait]
impl EventHandling for TokensWorkFlow {
    type U = CoreData;
    type D = ();

    async fn new(pgconf: &PostgresConfig) -> Self {
        let store = Store::new(pgconf, &store::SCHEMA).await;
        let parser = Parser::new();
        Self { parser, store }
    }

    async fn include_block(&mut self, data: &StampedData<CoreData>) -> Self::D {
        // First, get diff records
        let diff_records = self.parser.extract_diffs(&data);
        // Then get current balances for all address/assets within block
        let balances = self
            .store
            .map_balance_records(parsing::diffed_address_assets(&diff_records))
            .await;
        let stamped_batch = self.parser.extract_batch(data, diff_records, balances);
        self.store.persist(&stamped_batch).await;
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

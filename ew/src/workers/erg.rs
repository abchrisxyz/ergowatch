/*
    Consumes diffs from erg_diffs to maintain unspent balances
    and some (cex-independent) metrics.
    Balances and metrics is same worker for now but will have to
    be separated ideally into erg_balances and erg_metrics1 workers.
    When doing so, erg_balances will have to be able to replay
    balances to allow for lagging downstream workers. Can be implemented
    by maintaining a dedicated balance table for each lagging cursor.
    Those tables can be initialized through a query to erg_diffs to
    obtain balances for a cursor's starting height.
    When cursors can be merged, could assert dedicated balance table is
    identical to main one.
*/

mod parsing;
mod store;
mod types;

use async_trait::async_trait;

use crate::config::PostgresConfig;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::StampedData;
use crate::framework::Workflow;
use crate::workers::erg_diffs::types::DiffData;
use parsing::Parser;
use store::Store;
use types::Batch;

const WORKER_ID: &'static str = "erg";

pub type Worker = crate::framework::Worker<ErgWorkFlow>;

pub struct ErgWorkFlow {
    parser: Parser,
    store: Store,
}

#[async_trait]
impl Workflow for ErgWorkFlow {
    type U = DiffData;
    type D = ();

    async fn new(pgconf: &PostgresConfig) -> Self {
        let store = Store::new(pgconf, &store::SCHEMA).await;
        let cache = store::load_parser_cache(store.get_client()).await;
        let parser = Parser::new(cache);
        Self { parser, store }
    }

    async fn include_block(&mut self, data: &StampedData<DiffData>) -> Self::D {
        // Get current balances for all addresses within block
        let balances = self
            .store
            .map_balance_records(data.data.diffed_addresses())
            .await;
        let stamped_batch = self.parser.extract_batch(data, balances);
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

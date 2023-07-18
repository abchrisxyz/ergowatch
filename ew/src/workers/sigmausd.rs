mod parsing;
mod store;
mod types;

use crate::config::PostgresConfig;
use crate::core::types::Head;

use parsing::Parser;
use store::Store;
use types::Batch;

pub type Worker = super::Worker<SigmaUSD>;

// SigmaUSD V2 launched at height 453064.
// Starting a bit earlier to enusure we have valid oracle data when reaching 453064.
// const START_HEIGHT: Height = 450000;

pub struct Cache {}

pub struct SigmaUSD {
    cache: Cache,
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
        let cache = Cache {};
        let store = Store::new(pgconf.clone()).await;
        let parser = Parser::new();
        Self {
            cache,
            parser,
            store,
        }
    }
    async fn include_genesis_boxes(&mut self, boxes: &Vec<Output>) {
        todo!();
    }
    async fn include_block(&mut self, data: &CoreData) {
        // let state = store.read_state().await;
        // let batch = self.parser.dev(state, data).await;
        // self.store.persist(batch)
        todo!();
    }
    async fn roll_back(&mut self, height: Height) {
        todo!();
    }

    async fn head(&self) -> Head {
        todo!()
    }
}

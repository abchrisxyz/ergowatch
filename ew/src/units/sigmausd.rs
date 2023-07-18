mod store;
mod types;

use crate::config::PostgresConfig;
use crate::core::tracking::Tracker;
use crate::core::types::CoreData;
use crate::core::types::Output;

use super::Parser;
use super::Worker;
use store::SigStore;

// SigmaUSD V2 launched at height 453064.
// Starting a bit earlier to enusure we have valid oracle data when reaching 453064.
// const START_HEIGHT: Height = 450000;

/// Data extracted from a block and ready to be stored.
pub struct Batch {
    pub i: i32,
}

pub type SigWorker = Worker<SigParser, SigStore>;

pub struct SigParser;

impl Parser for SigParser {
    type B = Batch;

    fn parse_genesis_boxes(&self, outputs: &Vec<Output>) -> Self::B {
        todo!()
    }

    fn parse(&self, data: &CoreData) -> Batch {
        Batch { i: 3 }
    }
}

impl SigWorker {
    pub async fn new(id: &str, pgconf: &PostgresConfig, tracker: &mut Tracker) -> Self {
        let store = SigStore::new(pgconf.clone()).await;
        let head = store.get_head();
        let rx = tracker.add_cursor(id.to_owned(), head.clone());
        Self {
            id: String::from(id),
            rx,
            parser: SigParser {},
            store,
        }
    }
}

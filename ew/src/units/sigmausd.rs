mod store;

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;

use crate::config::PostgresConfig;
use crate::core::tracking::Tracker;
use crate::core::tracking::TrackingMessage;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::core::types::Output;

use super::Unit;
use store::Store;

pub struct Worker {
    id: String,
    store: Store,
    rx: Receiver<TrackingMessage>,
}

#[async_trait]
impl Unit for Worker {
    async fn new(id: &str, pgconf: &PostgresConfig, tracker: &mut Tracker) -> Self {
        let store = Store::new(pgconf.clone());
        let head = store.head().await;
        Self {
            id: String::from(id),
            store: store,
            rx: tracker.add_cursor(id.to_owned(), head),
        }
    }

    async fn next(&mut self) -> Option<TrackingMessage> {
        self.rx.recv().await
    }

    async fn handle_genesis(&mut self, genesis_blocks: Vec<Output>) {
        tracing::warn!("todo - handle genesis blocks")
    }

    async fn include(&mut self, data: &CoreData) {
        tracing::warn!("todo - handle block {}", data.block.header.height);
    }

    async fn roll_back(&mut self, height: Height) {
        todo!();
    }
}

use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;

use crate::config::PostgresConfig;
use crate::core::tracking::Tracker;
use crate::core::tracking::TrackingMessage;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::core::types::Output;

pub mod sigmausd;

// /// Helper function to register worker with tracker
// async fn attach(unit: &mut impl Unit, tracker: &mut Tracker) {
//     let head = unit.head().await;
//     let name = String::from("name");
//     let rx = tracker.add_cursor(name, head);
//     unit.set_rx(rx)
// }

#[async_trait]
pub trait Unit {
    async fn new(name: &str, pgconf: &PostgresConfig, tracker: &mut Tracker) -> Self;

    fn name(&self) -> String {
        String::from("blabla")
    }

    // /// Registers workers to receive events from tracker.
    // async fn attach(&mut self, tracker: &mut Tracker) {
    //     let head = self.head().await;
    //     let rx = tracker.add_cursor(self.name(), head);
    // }

    /// Run the worker
    // async fn start(mut self);

    async fn start(&mut self) {
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("got a ctrl-c message");
                    break;
                },
                msg = self.next() => {
                    match msg.expect("message is some") {
                        TrackingMessage::Genesis(blocks) => &mut self.handle_genesis(blocks).await,
                        TrackingMessage::Include(data) => &mut self.include(&data).await,
                        TrackingMessage::Rollback(height) => &mut self.roll_back(height).await,
                    };
                },
            }
        }
    }

    /// Returns current head
    // async fn head(&self) -> Head;

    async fn next(&mut self) -> Option<TrackingMessage>;

    async fn handle_genesis(&mut self, genesis_blocks: Vec<Output>);

    async fn include(&mut self, data: &CoreData);

    async fn roll_back(&mut self, height: Height);
}

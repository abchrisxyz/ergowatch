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

/// Workflows extract and store domain specfic data.
#[async_trait]
pub trait Workflow {
    /// Create and initialize and new workflow.
    async fn new(pgconf: &PostgresConfig) -> Self;

    /// Handle genesis boxes.
    async fn include_genesis_boxes(&mut self, boxes: &Vec<Output>);

    /// Process new block data.
    async fn include_block(&mut self, data: &CoreData);

    /// Roll back a block.
    async fn roll_back(&mut self, height: Height);

    /// Get last processed head.
    async fn head(&self) -> Head;
}

/// Workers listen to tracker events and drive a workflow.
pub struct Worker<W: Workflow> {
    id: String,
    rx: Receiver<TrackingMessage>,
    workflow: W,
}

impl<W: Workflow> Worker<W> {
    pub async fn new(id: &str, pgconf: &PostgresConfig, tracker: &mut Tracker) -> Self {
        let workflow = W::new(pgconf).await;
        let head = workflow.head().await;
        let rx = tracker.add_cursor(id.to_owned(), head.clone());

        Self {
            id: String::from(id),
            rx,
            workflow,
        }
    }

    pub async fn start(&mut self) {
        loop {
            tokio::select! {
                    _ = tokio::signal::ctrl_c() => {
                        tracing::info!("[{}] got a ctrl-c message", self.id);
                        break;
                },
                msg = self.rx.recv() => {
                    match msg.expect("message is some") {
                        TrackingMessage::Genesis(boxes) => self.workflow.include_genesis_boxes(&boxes).await,
                        TrackingMessage::Include(data) => self.workflow.include_block(&data).await,
                        TrackingMessage::Rollback(height) => self.workflow.roll_back(height).await,
                    };
                },
            }
        }
    }
}

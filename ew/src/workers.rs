use async_trait::async_trait;

use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use crate::config::PostgresConfig;
use crate::core::tracking::Tracker;
use crate::core::tracking::TrackingMessage;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::core::types::Output;
use crate::monitor::MonitorMessage;

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
    fn head<'a>(&'a self) -> &'a Head;
}

/// Workers listen to tracker events and drive a workflow.
pub struct Worker<W: Workflow> {
    id: String,
    rx: Receiver<TrackingMessage>,
    workflow: W,
    monitor_tx: Sender<MonitorMessage>,
}

impl<W: Workflow> Worker<W> {
    pub async fn new(
        id: &str,
        pgconf: &PostgresConfig,
        tracker: &mut Tracker,
        monitor_tx: Sender<MonitorMessage>,
    ) -> Self {
        let workflow = W::new(pgconf).await;
        let head = workflow.head();
        let rx = tracker.add_cursor(id.to_owned(), head.clone(), &monitor_tx);

        Self {
            id: String::from(id),
            rx,
            workflow,
            monitor_tx,
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
                        TrackingMessage::Include(data) => {
                            let head = self.workflow.head();
                            // Capped cursor may dispatch events prior to workflow's head. Ignore them.
                            if data.block.header.height <= head.height {continue;}
                            // Check next block is indeed child of last included one
                            assert_eq!(data.block.header.height, head.height + 1);
                            assert_eq!(data.block.header.parent_id, head.header_id);
                            // All good, proceed
                            let height = head.height;
                            self.workflow.include_block(&data).await;
                            self.monitor_tx.send(MonitorMessage::WorkerUpdate(height)).await.unwrap();
                        },
                        TrackingMessage::Rollback(height) => {
                            let head = self.workflow.head();
                            assert_eq!(height, head.height);
                            self.workflow.roll_back(height).await
                        },
                    };
                },
            }
        }
    }
}

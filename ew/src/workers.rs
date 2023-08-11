use async_trait::async_trait;

use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use crate::config::PostgresConfig;
use crate::core::tracking::Tracker;
use crate::core::tracking::TrackingMessage;
use crate::core::types::BoxData;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::monitor::MonitorMessage;
use crate::monitor::WorkerMessage;

pub mod sigmausd;

/// Workflows extract and store domain specfic data.
#[async_trait]
pub trait Workflow {
    /// Create and initialize and new workflow.
    async fn new(pgconf: &PostgresConfig) -> Self;

    /// Handle genesis boxes.
    async fn include_genesis_boxes(&mut self, boxes: &Vec<BoxData>);

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
        let mut workflow = W::new(pgconf).await;

        // Ensure the workflow head is on the main chain.
        // A worker could crash on a rollback while the tracker gets passed it.
        // In such a case, the workflow's head wouldn't be on the main chain anymore.
        // Here, we check for such cases and roll back the workflow until bacn
        // on the main chain again.
        // Skip this is if the workflow is ahead of the tracker.
        if workflow.head().height <= tracker.head().height {
            // Rolling back any blocks past the split.
            while !tracker.contains_head(workflow.head()).await {
                tracing::info!(
                    "workflow `{}` is not on main chain - rolling back {:?}",
                    &id,
                    workflow.head(),
                );
                workflow.roll_back(workflow.head().height).await;
            }
        }

        let rx = tracker.add_cursor(id.to_owned(), workflow.head().clone(), &monitor_tx);

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
                    match msg.unwrap() {
                        TrackingMessage::Genesis(boxes) => self.workflow.include_genesis_boxes(&boxes).await,
                        TrackingMessage::Include(data) => {
                            let head = self.workflow.head();
                            // Capped cursor may dispatch events prior to workflow's head. Ignore them.
                            if data.block.header.height <= head.height {continue;}
                            // Check next block is indeed child of last included one
                            assert_eq!(data.block.header.height, head.height + 1);
                            assert_eq!(data.block.header.parent_id, head.header_id);
                            // All good, proceed
                            self.workflow.include_block(&data).await;
                        },
                        TrackingMessage::Rollback(height) => {
                            let head = self.workflow.head();
                            assert_eq!(height, head.height);
                            self.workflow.roll_back(height).await
                        },
                    };
                    self.monitor_tx.send(
                        MonitorMessage::Worker(
                            WorkerMessage::new(
                                self.id.clone(),
                                self.workflow.head().height
                            )
                        )
                    ).await.unwrap();
                },
            }
        }
    }
}

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

use super::query_emission::QueryWrapper;

/// Query handling interface of workflows
#[async_trait]
pub trait QueryHandling {
    type Q: Send + Sync + std::fmt::Debug; // Query type
    type R: Send + Sync + std::fmt::Debug; // Query response type

    async fn execute(&self, query: Self::Q) -> Self::R;
}

pub(super) struct QueryHandler<W: QueryHandling> {
    id: &'static str,
    /// Query sender - to be cloned and passed to clients)
    query_tx: mpsc::Sender<QueryWrapper<W::Q, W::R>>,
    /// Query receiver - listens for incoming queries
    query_rx: mpsc::Receiver<QueryWrapper<W::Q, W::R>>,
}

impl<W: QueryHandling> QueryHandler<W> {
    /// Create a new QueryHandler.
    ///
    /// * `id` - name of the worker
    pub(super) fn new(id: &'static str) -> Self {
        let (query_tx, query_rx) = mpsc::channel(8);
        Self {
            id,
            query_tx,
            query_rx,
        }
    }

    /// Returns an MPSC sender for a client to send queries through.
    pub fn connect(&self) -> mpsc::Sender<QueryWrapper<W::Q, W::R>> {
        tracing::debug!("[{}] providing a connection to query handler", self.id);
        self.query_tx.clone()
    }

    pub async fn recv(&mut self) -> Option<QueryWrapper<W::Q, W::R>> {
        self.query_rx.recv().await
    }

    pub async fn handle_qw(&mut self, qw: QueryWrapper<W::Q, W::R>, workflow: &W) {
        let response = workflow.execute(qw.query).await;
        qw.response_tx.send(response).unwrap();
    }

    /// Handle any pending queries
    pub async fn handle_pending(&mut self, workflow: &W) {
        loop {
            match self.query_rx.try_recv() {
                Ok(qw) => {
                    let response = workflow.execute(qw.query).await;
                    qw.response_tx.send(response).unwrap();
                }
                Err(TryRecvError::Empty) => {
                    // No pending queries, move on.
                    tracing::trace!("No pending queries");
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    tracing::warn!("Query source is down :(");
                    break;
                }
            }
        }
    }
}

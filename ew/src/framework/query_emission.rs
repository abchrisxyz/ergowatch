use async_trait::async_trait;

use tokio::sync::mpsc;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct QueryWrapper<Q: std::fmt::Debug, R: std::fmt::Debug> {
    pub query: Q,
    pub response_tx: oneshot::Sender<R>,
}

/// Util to facilitate query sending.
pub struct QuerySender<Q: std::fmt::Debug, R: std::fmt::Debug> {
    tx: mpsc::Sender<QueryWrapper<Q, R>>,
}

impl<Q: std::fmt::Debug, R: std::fmt::Debug> QuerySender<Q, R> {
    /// Create a new query sender from provided MPSC sender.
    ///
    /// * `tx`: MPSC sender to channel owned by query handler
    pub fn new(tx: mpsc::Sender<QueryWrapper<Q, R>>) -> Self {
        Self { tx }
    }

    pub fn placeholder() -> Self {
        // Dummy placeholder query_tx
        let (tx, _) = mpsc::channel(1);
        Self { tx }
    }

    /// Sends query to query handler and returns a oneshot receiver
    /// through wich the query response can be received.
    pub async fn send(&self, query: Q) -> oneshot::Receiver<R> {
        tracing::debug!("sending query {query:?}");
        let (response_tx, response_rx) = oneshot::channel();
        let qw = QueryWrapper { query, response_tx };
        self.tx.send(qw).await.unwrap();
        response_rx
    }
}

/// Query emitting workflows
#[async_trait]
pub trait Querying {
    type Q: std::fmt::Debug;
    type R: std::fmt::Debug;

    fn set_query_sender(&mut self, query_sender: QuerySender<Self::Q, Self::R>);
}

// /// Query emitting workers
// #[async_trait]
// pub trait QueryingWorker {
//     type W: QueryableSourceWorker; // Queried worker type

//     fn connect_to_query_handler(&mut self, target: &Self::W) {
//         let query_sender = target.connect();
//     }
// }

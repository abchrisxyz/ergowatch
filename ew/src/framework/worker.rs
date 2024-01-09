use async_trait::async_trait;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use super::event::Event;

use super::event_emission::EventEmission;
use super::event_emission::EventEmitter;
use super::event_handling::EventHandler;
use super::event_handling::EventHandling;
use super::event_handling::FwdEventHandler;
use super::query_emission::Querying;
use super::query_handling::QueryHandler;
use super::Source;
use crate::config::PostgresConfig;
use crate::core::types::Header;
use crate::monitor::MonitorMessage;

pub struct LeafWorker<W: EventHandling> {
    id: &'static str,
    event_handler: EventHandler<W>,
}

impl<W: EventHandling> LeafWorker<W> {
    /// Create a new LeafWorker.
    ///
    /// * `id` - name of the worker
    /// * `pgconf` - postgres connection details
    /// * `source` - the upstream source to track
    /// * `monitor_tx` - a monitor channel
    pub async fn new(
        id: &'static str,
        pgconf: &PostgresConfig,
        source: &mut impl Source<S = W::U>,
        monitor_tx: Sender<MonitorMessage>,
    ) -> Self {
        let event_handler = EventHandler::new(id, pgconf, source, monitor_tx).await;
        Self { id, event_handler }
    }

    #[tracing::instrument(name="worker", skip(self), fields(worker=self.id))]
    pub async fn start(&mut self) {
        tracing::info!("starting");
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("[{}] got a ctrl-c message", self.id);
                    todo!("Handle propagate ctrl-c");
                },
                _ = self.event_handler.recv_and_handle() => {}
            }
        }
    }
}

impl<W: EventHandling + Querying> LeafWorker<W> {
    /// Configures the query sender of a querying workflow.
    pub fn connect_query_sender<T: QueryHandler<Q = W::Q, R = W::R>>(&mut self, target: &T) {
        let query_sender = target.connect();
        self.event_handler.set_query_sender(query_sender);
    }
}

// SourceWorker ----------------------------------------------------------------
pub struct SourceWorker<W: EventHandling + EventEmission<S = W::D>> {
    id: &'static str,
    event_handler: FwdEventHandler<W>,
    event_emitter: EventEmitter<W>,
}

impl<W: EventHandling + EventEmission<S = W::D>> SourceWorker<W> {
    /// Create a new SourceWorker.
    ///
    /// * `id` - name of the worker
    /// * `pgconf` - postgres connection details
    /// * `source` - the upstream source to track
    /// * `monitor_tx` - a monitor channel
    pub async fn new(
        id: &'static str,
        pgconf: &PostgresConfig,
        source: &mut impl Source<S = W::U>,
        monitor_tx: Sender<MonitorMessage>,
    ) -> Self {
        let event_handler = FwdEventHandler::new(id, pgconf, source, monitor_tx).await;
        let event_emitter = EventEmitter::new();
        Self {
            id,
            event_handler,
            event_emitter,
        }
    }

    #[tracing::instrument(name="worker", skip(self), fields(worker=self.id))]
    pub async fn start(&mut self) {
        tracing::info!("starting");
        // Progress lagging cursors while waiting for new blocks
        let n = 10;
        while self.event_emitter.has_lagging_cursors() {
            // Handle up to `n` upstream events
            for _ in 0..n {
                match self.event_handler.try_next().await {
                    Ok(handled_event) => self.event_emitter.forward(handled_event).await,
                    Err(TryRecvError::Empty) => {
                        // No events from upstream, move on.
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        tracing::warn!("Upstream is down :( stopping this worker");
                        return;
                    }
                }
            }
            // step lagging cursors by `n` and merge where possible
            self.event_emitter
                .progress_lagging_cursors(self.event_handler.workflow(), n)
                .await;
        }

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("[{}] got a ctrl-c message", self.id);
                    break;
                },
                msg = self.event_handler.recv() => {
                    let event = msg.unwrap();
                    let handled_event = self.event_handler.handle_event(event).await;
                    self.event_emitter.forward(handled_event).await;
                }
            }
        }
    }
}

#[async_trait]
impl<W: EventHandling + EventEmission<S = W::D> + Send + Sync> Source for SourceWorker<W> {
    type S = W::D;

    fn header(&self) -> &Header {
        &self.event_handler.workflow().header()
    }

    async fn contains_header(&self, header: &Header) -> bool {
        self.event_handler.workflow().contains_header(header).await
    }

    // TODO: cursor name should not be set by caller
    async fn subscribe(&mut self, header: Header, cursor_name: &str) -> Receiver<Event<Self::S>> {
        self.event_emitter
            .subscribe(
                header,
                cursor_name,
                self.event_handler.workflow(),
                self.event_handler.monitor_tx(),
            )
            .await
    }
}

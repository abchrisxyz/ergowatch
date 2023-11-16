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
use super::query_emission::QuerySender;
use super::query_emission::Querying;
use super::query_handling::QueryHandler;
use super::query_handling::QueryHandling;
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

    pub async fn start(&mut self) {
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
    pub fn connect_query_sender<T>(&mut self, target: &QueryableSourceWorker<T>)
    where
        T: EventHandling + EventEmission<S = T::D> + QueryHandling<Q = W::Q, R = W::R>,
    {
        let query_sender = target.create_query_sender();
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

    pub async fn start(&mut self) {
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

// QueryableSourceWorker ----------------------------------------------------------------
pub struct QueryableSourceWorker<W: EventHandling + EventEmission<S = W::D> + QueryHandling> {
    id: &'static str,
    event_handler: FwdEventHandler<W>,
    event_emitter: EventEmitter<W>,
    query_handler: QueryHandler<W>,
}

impl<W: EventHandling + EventEmission<S = W::D> + QueryHandling> QueryableSourceWorker<W> {
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
        let query_handler = QueryHandler::new(id);
        Self {
            id,
            event_handler,
            event_emitter,
            query_handler,
        }
    }

    /// Returns a `QuerySender` sender for a client to send queries through.
    pub fn create_query_sender(&self) -> QuerySender<W::Q, W::R> {
        QuerySender::new(self.query_handler.connect())
    }

    pub async fn start(&mut self) {
        // Progress lagging cursors while waiting for new blocks
        let n = 10;

        self.query_handler
            .handle_pending(self.event_handler.workflow())
            .await;

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
                // Biased polling order to ensure pending queries are handled
                // before new events.
                biased;
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("[{}] got a ctrl-c message", self.id);
                    break;
                },
                msg = self.query_handler.recv() => {
                    let qw = msg.unwrap();
                    self.query_handler.handle_qw(qw, self.event_handler.workflow()).await;
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

// TODO: this duplictes impl Source for SourceWorker
#[async_trait]
impl<W: EventHandling + EventEmission<S = W::D> + QueryHandling + Send + Sync> Source
    for QueryableSourceWorker<W>
{
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

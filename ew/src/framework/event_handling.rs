use async_trait::async_trait;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use super::event::Event;
use super::event::HandledEvent;
use super::event::StampedData;
use super::query_emission::QuerySender;
use super::query_emission::Querying;
use super::Source;
use crate::config::PostgresConfig;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::monitor::MonitorMessage;
use crate::monitor::WorkerMessage;

/// Event handling interface of workflows
#[async_trait]
pub trait EventHandling {
    type U: Send + Sync; // upstream data
    type D: Send + Sync; // downstream data () for a sink

    /// Create and initialize a new event handling workflow.
    async fn new(pgconf: &PostgresConfig) -> Self;

    /// Process new block data.
    async fn include_block(&mut self, data: &StampedData<Self::U>) -> Self::D;

    /// Roll back a block and return previous head.
    async fn roll_back(&mut self, height: Height) -> Header;

    /// Get last processed header.
    fn header<'a>(&'a self) -> &'a Header;
}

pub struct EventHandler<W: EventHandling> {
    id: &'static str,
    workflow: W,
    rx: Receiver<Event<W::U>>,
    monitor_tx: Sender<MonitorMessage>,
}

impl<W: EventHandling> EventHandler<W> {
    /// Create a new EventHandler.
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
        let workflow = W::new(pgconf).await;
        Self::new_with(id, workflow, source, monitor_tx).await
    }

    /// Create a new EventHandler with given `workflow`.
    ///
    /// * `id` - name of the worker
    /// * `workflow` - the workflow to be driven by new event handler
    /// * `source` - the upstream source to track
    /// * `monitor_tx` - a monitor channel
    ///
    /// TODO: Consider replacing usage of new by new_with (more flexible) and
    /// then remove W::new from the Workflow trait.
    pub async fn new_with(
        id: &'static str,
        mut workflow: W,
        source: &mut impl Source<S = W::U>,
        monitor_tx: Sender<MonitorMessage>,
    ) -> Self {
        // Ensure workflow is on main chain
        Self::ensure_main_chain(id, &mut workflow, source).await;
        // Subscribe to source from current position
        let rx = source.subscribe(workflow.header().clone(), id).await;

        Self {
            id,
            rx,
            workflow,
            monitor_tx,
        }
    }

    /// Wait for and handle next event.
    ///
    /// Consider using lower level functions when behind a tokio::select!
    /// blocks with multiple branches to avoid event drops from cancellation.
    pub async fn recv_and_handle(&mut self) {
        let event = self.rx.recv().await.unwrap();
        self.process_upstream_event(&event).await;
    }

    /// Wait for return next event.
    pub async fn recv(&mut self) -> Option<Event<W::U>> {
        self.rx.recv().await
    }

    /// Try reveiving a pending event.
    ///
    /// Does not wait for events if none pending.
    pub fn try_recv(&mut self) -> Result<Event<W::U>, TryRecvError> {
        self.rx.try_recv()
    }

    pub async fn process_upstream_event(&mut self, event: &Event<W::U>) {
        match event {
            Event::Include(stamped_data) => {
                // Capped cursor may dispatch events prior to workflow's head. Ignore them.
                if stamped_data.height <= self.workflow.header().height {
                    return;
                }
                self.handle_include(&stamped_data).await;
            }
            Event::Rollback(height) => {
                self.handle_rollback(*height).await;
            }
        };
        self.report_status().await;
    }

    /// Ensure the workflow head is on the main chain.
    async fn ensure_main_chain(
        id: &'static str,
        workflow: &mut W,
        source: &mut impl Source<S = W::U>,
    ) {
        // A worker could crash on a rollback while the tracker gets passed it.
        // In such a case, the workflow's head wouldn't be on the main chain anymore.
        // Here, we check for such cases and roll back the workflow until back
        // on the main chain again.
        // Skip this is if the workflow is ahead of the tracker.
        if workflow.header().height <= source.header().height {
            // Rolling back any blocks past the split.
            while !source.contains_header(workflow.header()).await {
                tracing::info!(
                    "workflow `{}` is not on main chain - rolling back {:?}",
                    &id,
                    workflow.header(),
                );
                workflow.roll_back(workflow.header().height).await;
            }
        }
    }

    async fn handle_include(&mut self, payload: &StampedData<W::U>) -> W::D {
        tracing::debug!("handling new block {}", payload.height);
        // Check next block is indeed child of last included one
        let head = self.workflow.header();
        assert_eq!(payload.height, head.height + 1);
        assert_eq!(payload.parent_id, head.header_id);
        // All good, proceed
        self.workflow.include_block(&payload).await
    }

    async fn handle_rollback(&mut self, height: Height) -> Header {
        assert_eq!(height, self.workflow.header().height);
        self.workflow.roll_back(height).await
    }

    /// Reports worker's status to monitor.
    async fn report_status(&self) {
        self.monitor_tx
            .send(MonitorMessage::Worker(WorkerMessage::new(
                self.id,
                self.workflow.header().height,
            )))
            .await
            .unwrap();
    }
}

impl<W: EventHandling + Querying> EventHandler<W> {
    pub fn set_query_sender(&mut self, query_sender: QuerySender<W::Q, W::R>) {
        self.workflow.set_query_sender(query_sender);
    }
}

pub(super) struct FwdEventHandler<W: EventHandling> {
    pub(super) base_handler: EventHandler<W>,
}

impl<W: EventHandling> FwdEventHandler<W> {
    /// Create a new FwdEventHandler.
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
        let base_handler = EventHandler::new(id, pgconf, source, monitor_tx).await;
        Self { base_handler }
    }

    pub(super) fn workflow(&self) -> &W {
        &self.base_handler.workflow
    }

    pub(super) fn monitor_tx(&self) -> &Sender<MonitorMessage> {
        &self.base_handler.monitor_tx
    }

    /// Wait for and return next event.
    pub async fn recv(&mut self) -> Option<Event<W::U>> {
        self.base_handler.recv().await
    }

    /// Handle given `event` and return resulting event for downstream workers.
    pub async fn handle_event(&mut self, event: Event<W::U>) -> HandledEvent<W::D> {
        self.process_upstream_event(&event).await
    }

    /// Try reveiving and handling a pending event.
    ///
    /// Does not wait for events if none pending.
    pub async fn try_next(&mut self) -> Result<HandledEvent<W::D>, TryRecvError> {
        let event = self.base_handler.try_recv()?;
        Ok(self.process_upstream_event(&event).await)
    }

    async fn process_upstream_event(&mut self, event: &Event<W::U>) -> HandledEvent<W::D> {
        let ds_event = match event {
            Event::Include(stamped_data) => {
                // Capped cursor may dispatch events prior to workflow's head. Ignore them.
                if stamped_data.height <= self.base_handler.workflow.header().height {
                    return HandledEvent::Skipped;
                }
                let downstream_data = self.base_handler.handle_include(&stamped_data).await;
                HandledEvent::Include(stamped_data.wrap(downstream_data))
            }
            Event::Rollback(height) => {
                let prev_header = self.base_handler.handle_rollback(*height).await;
                HandledEvent::Rollback(prev_header)
            }
        };
        self.base_handler.report_status().await;
        ds_event
    }
}

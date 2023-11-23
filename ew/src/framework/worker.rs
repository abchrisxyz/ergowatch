use async_trait::async_trait;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::Sender;

use super::event::Event;
use super::event::StampedData;
use super::Cursor;
use super::Source;
use super::Sourceable;
use super::Workflow;
use crate::config::PostgresConfig;
use crate::core::types::Header;
use crate::monitor::MonitorMessage;
use crate::monitor::WorkerMessage;

pub struct Worker<W: Workflow> {
    id: String,
    workflow: W,
    rx: Receiver<Event<W::U>>,
    monitor_tx: Sender<MonitorMessage>,
}

impl<W: Workflow> Worker<W> {
    /// Create a new SourceWorker.
    ///
    /// * `id` - name of the worker
    /// * `pgconf` - postgres connection details
    /// * `source` - the upstream source to track
    /// * `monitor_tx` - a monitor channel
    pub async fn new(
        id: &str,
        pgconf: &PostgresConfig,
        source: &mut impl Source<S = W::U>,
        monitor_tx: Sender<MonitorMessage>,
    ) -> Self {
        let mut workflow = W::new(pgconf).await;

        // Ensure the workflow head is on the main chain.
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

        // let rx = tracker.add_cursor(id.to_owned(), workflow.head().clone(), &monitor_tx);
        let rx = source.subscribe(workflow.header().clone(), id).await;

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
                    self.process_upstream_event(msg.unwrap()).await
                },
            }
        }
    }

    async fn process_upstream_event(&mut self, event: Event<W::U>) {
        match event {
            Event::Include(payload) => {
                // Capped cursor may dispatch events prior to workflow's head. Ignore them.
                if payload.height <= self.workflow.header().height {
                    return;
                }
                self.include(&payload).await;
            }
            Event::Rollback(height) => {
                assert_eq!(height, self.workflow.header().height);
                self.workflow.roll_back(height).await;
            }
        };
        self.report_status().await;
    }

    async fn include(&mut self, payload: &StampedData<W::U>) -> W::D {
        // Check next block is indeed child of last included one
        let head = self.workflow.header();
        assert_eq!(payload.height, head.height + 1);
        assert_eq!(payload.parent_id, head.header_id);
        // All good, proceed
        let d = self.workflow.include_block(&payload).await;
        d
    }

    /// Reports worker's status to monitor.
    async fn report_status(&self) {
        self.monitor_tx
            .send(MonitorMessage::Worker(WorkerMessage::new(
                self.id.clone(),
                self.workflow.header().height,
            )))
            .await
            .unwrap();
    }
}

pub struct SourceWorker<W: Workflow + Sourceable<S = W::D>> {
    worker: Worker<W>,
    /// A tracking cursor is at the same position as the source.
    ///
    /// Incoming events get forwarded to the tracking cursor,
    /// if there is one.
    tracking_cursor: Option<Cursor<W::D>>,
    /// A lagging cursor is still catching up with its source.
    ///
    /// A source can have multiple lagging cursors, each at different
    /// positions. Lagging cursors are progressed independently of what
    /// happens upstream. Eventually, all lagging cursors will catch up
    /// and get merged into a single tracking cursor.
    lagging_cursors: Vec<Cursor<W::D>>,
}

impl<W: Workflow + Sourceable<S = W::D>> SourceWorker<W> {
    /// Create a new SourceWorker.
    ///
    /// * `id` - name of the worker
    /// * `pgconf` - postgres connection details
    /// * `source` - the upstream source to track
    /// * `monitor_tx` - a monitor channel
    pub async fn new(
        id: &str,
        pgconf: &PostgresConfig,
        source: &mut impl Source<S = W::U>,
        monitor_tx: Sender<MonitorMessage>,
    ) -> Self {
        Self {
            worker: Worker::new(id, pgconf, source, monitor_tx).await,
            tracking_cursor: None,
            lagging_cursors: vec![],
        }
    }

    pub async fn start(&mut self) {
        // Progress lagging cursors while waiting for new blocks
        let n = 10;
        while !self.lagging_cursors.is_empty() {
            for _ in 0..n {
                match self.worker.rx.try_recv() {
                    Ok(event) => self.process_upstream_event(event).await,
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
            // step lagging cursors by n and merge if possible
            self.progress_lagging_cursors(n).await;
        }

        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("[{}] got a ctrl-c message", self.worker.id);
                    break;
                },
                event = self.worker.rx.recv() => {
                    self.process_upstream_event(event.unwrap()).await
                }
            }
        }
    }

    async fn process_upstream_event(&mut self, event: Event<W::U>) {
        match event {
            Event::Include(stamped_data) => {
                // Capped cursor may dispatch events prior to workflow's head. Ignore them.
                if stamped_data.height <= self.worker.workflow.header().height {
                    return;
                }
                let downstream_data = self.worker.include(&stamped_data).await;
                if let Some(ref mut cursor) = self.tracking_cursor {
                    cursor.include(stamped_data.wrap(downstream_data)).await;
                }
            }
            Event::Rollback(height) => {
                let prev = self.worker.workflow.roll_back(height).await;
                if let Some(ref mut cursor) = self.tracking_cursor {
                    cursor.roll_back(prev).await;
                }
            }
        }
        self.worker.report_status().await;
    }

    /// Step lagging cursors by n blocks
    async fn progress_lagging_cursors(&mut self, n: i32) {
        // In any case, do not go past current source position
        let max_height = self.worker.workflow.header().height;

        for cursor in &mut self.lagging_cursors {
            let steps = std::cmp::min(n, max_height - cursor.header.height);
            for _ in 0..steps {
                let height = cursor.header.height + 1;
                let data = self.worker.workflow.get_at(height).await;
                cursor.include(data).await;
            }
        }
        // merge cursors where possible
        self.merge_cursors().await;
    }

    /// Attempts to merge cursors when at the same height
    async fn merge_cursors(&mut self) {
        // Check if any of the lagging cursors is at source's current
        // height and merge them with tracking cursors, if any, otherwise
        // make it the tracking cursor.

        // Collect indices of lagging cursors ready to be merged
        let current_head = self.worker.workflow.header();
        let ready: Vec<usize> = self
            .lagging_cursors
            .iter()
            .enumerate()
            .filter(|(_i, c)| c.is_at(current_head))
            .map(|(i, _c)| i)
            .collect();

        for cursor_index in ready {
            let other = self.lagging_cursors.remove(cursor_index);
            if self.tracking_cursor.is_some() {
                // There already is a tracking cursor, so merge the other into it.
                tracing::info!("Merging cursor {} with main one.", &other.id);
                self.tracking_cursor.as_mut().unwrap().merge(other).await;
            } else {
                // No tracking cursor yet, make this one the tracking one.
                tracing::info!("Making cursor {} the tracking one.", &other.id);
                self.tracking_cursor = Some(other.rename("main"));
            }
        }
    }
}

#[async_trait]
impl<W: Workflow + Sourceable<S = W::D> + Send + Sync> Source for SourceWorker<W> {
    type S = W::D;

    fn header(&self) -> &Header {
        &self.worker.workflow.header()
    }

    async fn contains_header(&self, header: &Header) -> bool {
        self.worker.workflow.contains_header(header).await
    }

    // TODO: cursor name should not be set by caller
    async fn subscribe(&mut self, header: Header, cursor_name: &str) -> Receiver<Event<Self::S>> {
        // Create new channel
        let (tx, rx) = tokio::sync::mpsc::channel(super::EVENT_CHANNEL_CAPACITY);

        // Workflows may start at a non-zero height and ignore/skip any blocks
        // prior. The source store could be empty or not having reached the
        // downstream workflow's start height yet. Because a cursor cannot point
        // past its worker's head, we cap it to the current worker's head if needed.
        let max_header = self.worker.workflow.header().clone();
        let capped_header = if header.height > max_header.height {
            tracing::info!(
                "cursor [{}] is ahead of tracker - using tracker's height",
                cursor_name
            );
            max_header
        } else {
            header
        };

        let is_tracking = &capped_header == self.worker.workflow.header();

        let make_cursor = |tx| -> Cursor<Self::S> {
            Cursor {
                id: cursor_name.to_owned(),
                header: capped_header.clone(),
                txs: vec![tx],
                monitor_tx: self.worker.monitor_tx.clone(),
            }
        };

        // If at tracking position, then add/put into tracking
        if is_tracking {
            if let Some(ref mut cursor) = self.tracking_cursor {
                cursor.txs.push(tx);
                return rx;
            } else {
                self.tracking_cursor = Some(make_cursor(tx));
                return rx;
            }
        }

        // Check if any of  the lagging cursors is at the same position
        for cursor in &mut self.lagging_cursors {
            if cursor.is_at(&capped_header) {
                cursor.txs.push(tx);
                return rx;
            }
        }

        // No existing cursors at same position, so make a new one.
        self.lagging_cursors.push(make_cursor(tx));

        rx
    }
}

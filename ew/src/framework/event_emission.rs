use async_trait::async_trait;
use tokio::sync::mpsc;

use super::event::Event;
use super::event::HandledEvent;
use super::event::StampedData;
use super::event_handling::EventHandling;
use super::utils::BlockRange;
use super::Cursor;
use crate::core::types::Header;
use crate::monitor::MonitorMessage;

#[async_trait]
pub trait EventEmission {
    type S;

    /// Returns true if data for `header` has been included.
    async fn contains_header(&self, header: &Header) -> bool;

    /// Get data for given height range.
    ///
    /// Used by lagging cursors to retrieve data.
    async fn get_slice(&self, blcok_range: &BlockRange) -> Vec<StampedData<Self::S>>;
}

pub(super) struct EventEmitter<W: EventHandling + EventEmission> {
    /// A tracking cursor is at the same position as the source.
    ///
    /// Incoming events get forwarded to the tracking cursor,
    /// if there is one.
    tracking_cursor: Option<Cursor<W::S>>,
    /// A lagging cursor is still catching up with its source.
    ///
    /// A source can have multiple lagging cursors, each at different
    /// positions. Lagging cursors are progressed independently of what
    /// happens upstream. Eventually, all lagging cursors will catch up
    /// and get merged into a single tracking cursor.
    lagging_cursors: Vec<Cursor<W::S>>,
}

impl<W: EventHandling + EventEmission> EventEmitter<W> {
    /// Create a new EventEmitter
    pub fn new() -> Self {
        Self {
            tracking_cursor: None,
            lagging_cursors: vec![],
        }
    }

    /// Returns true if emitter has any lagging cursors
    pub(super) fn has_lagging_cursors(&self) -> bool {
        !self.lagging_cursors.is_empty()
    }

    /// Forward a handled event to the tracking cursor.
    pub(super) async fn forward(&mut self, upstream_event: HandledEvent<W::S>) {
        match upstream_event {
            HandledEvent::Skipped => {
                // Skip
            }
            HandledEvent::Include(stamped_data) => {
                if let Some(ref mut cursor) = self.tracking_cursor {
                    cursor.include(stamped_data).await;
                }
            }
            HandledEvent::Rollback(header) => {
                if let Some(ref mut cursor) = self.tracking_cursor {
                    cursor.roll_back(header).await;
                }
            }
        }
    }

    /// Step lagging cursors by n blocks
    pub(super) async fn progress_lagging_cursors(&mut self, workflow: &W, n: i32) {
        // In any case, do not go past current source position
        let max_height = workflow.header().height;

        for cursor in &mut self.lagging_cursors {
            let steps = std::cmp::min(n, max_height - cursor.header.height);
            let first_height = cursor.header.height + 1;
            let last_height = cursor.header.height + steps;

            let block_range = BlockRange::new(first_height, last_height);
            let slice = workflow.get_slice(&block_range).await;
            for data in slice {
                cursor.include(data).await;
            }
        }
        // merge cursors where possible
        self.merge_cursors(workflow).await;
    }

    /// Attempts to merge cursors when at the same height
    async fn merge_cursors(&mut self, workflow: &W) {
        // Check if any of the lagging cursors is at source's current
        // height and merge them with tracking cursors, if any, otherwise
        // make it the tracking cursor.

        // Collect indices of lagging cursors ready to be merged
        let current_head = workflow.header();
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

    pub(super) async fn subscribe(
        &mut self,
        header: Header,
        cursor_name: &str,
        workflow: &W,
        monitor_tx: &mpsc::Sender<MonitorMessage>,
    ) -> mpsc::Receiver<Event<W::S>> {
        // Create new channel
        let (tx, rx) = tokio::sync::mpsc::channel(super::EVENT_CHANNEL_CAPACITY);

        // Workflows may start at a non-zero height and ignore/skip any blocks
        // prior. The source store could be empty or not having reached the
        // downstream workflow's start height yet. Because a cursor cannot point
        // past its worker's head, we cap it to the current worker's head if needed.
        let max_header = workflow.header().clone();
        let capped_header = if header.height > max_header.height {
            tracing::info!(
                "cursor [{}] is ahead of tracker - using tracker's height",
                cursor_name
            );
            max_header
        } else {
            header
        };

        let is_tracking = &capped_header == workflow.header();

        let make_cursor = |tx| -> Cursor<W::S> {
            Cursor {
                id: cursor_name.to_owned(),
                header: capped_header.clone(),
                txs: vec![tx],
                monitor_tx: monitor_tx.clone(),
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

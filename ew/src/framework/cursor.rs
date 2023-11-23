use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use super::event::Event;
use super::event::StampedData;
use crate::core::types::Header;
use crate::monitor::CursorMessage;
use crate::monitor::CursorRollback;
use crate::monitor::MonitorMessage;

pub struct Cursor<D> {
    pub id: String,
    pub header: Header,
    // status: Status,
    /// Collections of senders to listening workers
    pub txs: Vec<Sender<Event<D>>>,
    /// Sender for monitor channel
    pub monitor_tx: Sender<MonitorMessage>,
}

impl<D> Cursor<D> {
    /// Checks if cursor is at given position
    pub fn is_at(&self, header: &Header) -> bool {
        self.header == *header
    }

    /// Checks if cursor is at same position as other
    pub fn is_on(&self, other: &Self) -> bool {
        self.is_at(&other.header)
    }

    pub fn rename(mut self, id: &str) -> Self {
        self.id = id.to_owned();
        self
    }

    /// Merge other cursor into self.
    ///
    /// Other's channels are taken over by self.
    pub async fn merge(&mut self, mut other: Self) {
        tracing::info!("Merging cursors [{}] and [{}]", self.id, other.id);
        // Signal to monitor that the other cursor will get dropped.
        other
            .monitor_tx
            .send(MonitorMessage::CursorDrop(other.id))
            .await
            .unwrap();
        // Take over channels of other cursor.
        self.txs.append(&mut other.txs);
    }

    /// Sends data for inclusion to all listening channels.
    pub async fn include(&mut self, data: StampedData<D>) {
        tracing::info!(
            "[{}] including data for block {} {}",
            self.id,
            data.height,
            data.header_id
        );
        // assert_eq!(stamped_data.stamp.height, self.head.height + 1);
        // assert_eq!(stamped_data.stamp.parent_id, self.head.header_id);
        let new_header = Header {
            height: data.height,
            timestamp: data.timestamp,
            header_id: data.header_id.clone(),
            parent_id: data.parent_id.clone(),
        };
        self.send(Event::Include(Arc::new(data))).await;
        self.header = new_header;
        self.report_status().await;
    }

    pub async fn roll_back(&mut self, previous_header: Header) {
        tracing::warn!(
            "[{}] rolling back data for block {} {}",
            self.id,
            self.header.height,
            self.header.header_id
        );
        assert_eq!(previous_header.height, self.header.height - 1);

        // Report rollback to monitor
        self.monitor_tx
            .send(MonitorMessage::Rollback(CursorRollback::new(
                self.id.clone(),
                self.header.height,
            )))
            .await
            .unwrap();

        // Rollback event carries the height to be rolled back
        self.send(Event::Rollback(self.header.height)).await;
        self.header = previous_header;
        self.report_status().await;
    }

    async fn send(&mut self, event: Event<D>) {
        let mut broken_channel_indices: Vec<usize> = vec![];
        for (idx, tx) in self.txs.iter().enumerate() {
            match tx.send(event.shallow_copy()).await {
                Ok(_) => (),
                Err(e) => {
                    tracing::warn!("Send failed - Error was: {e}");
                    broken_channel_indices.push(idx);
                }
            };
        }
        if !broken_channel_indices.is_empty() {
            self.drop_broken_channels(broken_channel_indices);
        }
    }

    fn drop_broken_channels(&mut self, mut channel_indices: Vec<usize>) {
        channel_indices.sort();
        channel_indices.reverse();
        for i in channel_indices {
            tracing::warn!("dropping broken channel to downstream worker");
            self.txs.remove(i);
        }
    }

    async fn report_status(&self) {
        self.monitor_tx
            .send(MonitorMessage::Cursor(CursorMessage::new(
                self.id.clone(),
                self.header.height,
            )))
            .await
            .unwrap();
    }
}

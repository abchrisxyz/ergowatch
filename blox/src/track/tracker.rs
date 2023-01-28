use tokio::sync::mpsc;
use tracing::info;

use crate::node::Node;
use crate::track::cursor::Cursor;
use crate::track::messages::TrackingMessage;

/// The capacity of mpsc channels used to communicate tracking events
const CHANNEL_CAPACITY: usize = 8;

pub struct Tracker {
    node: Node,
    cursors: Vec<Cursor>,
}

impl Tracker {
    pub fn new(node: Node) -> Self {
        Self {
            node,
            cursors: vec![],
        }
    }

    pub fn add_cursor(
        &mut self,
        name: String,
        height: i32,
        header_id: String,
    ) -> mpsc::Receiver<TrackingMessage> {
        // Create new channel
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);

        // Check for existing cursors at same position
        for cur in &mut self.cursors {
            if cur.is_at(height, &header_id) {
                cur.txs.push(tx);
                return rx;
            }
        }

        let cur = Cursor {
            name,
            height,
            header_id,
            node: self.node.clone(),
            txs: vec![tx],
            polling_interval: tokio::time::Duration::from_millis(5000),
        };
        self.cursors.push(cur);
        rx
    }

    pub async fn start(&mut self) {
        if self.cursors.len() > 1 {
            self.join_cursors().await;
        }
        self.single_cursor().await;
    }

    /// Progresses multiple cursors until they're all at the same position.
    async fn join_cursors(&mut self) {
        loop {
            for cur in &mut self.cursors {
                cur.step().await;
            }
            self.merge_cursors();
            if self.cursors.len() == 1 {
                break;
            }
        }
    }

    /// Attempts to merge cursors when at the same height
    fn merge_cursors(&mut self) {
        // The new collection of cursors with just the first cursor, for now
        let mut merged: Vec<Cursor> = vec![self.cursors.pop().unwrap()];

        // Assume first cursor is highest
        let mut head = &mut merged[0];

        // Iterate over remaining cursor in existing collections
        while let Some(cur) = self.cursors.pop() {
            // If next cursor is higher, add to new collection and use that as head
            if cur.height > head.height {
                merged.push(cur);
                head = merged.last_mut().unwrap();
                // But, if encountering an identical cursor, merge it with head.
                // We only ever merge with head, so we could miss the opportunity
                // to merge identical cursors behind head. However, the chances of
                // this occuring are very slim.
            } else if cur.is_on(head) {
                info!("Merging cursors [{}] and [{}]", head.name, cur.name);
                head.merge(cur);
            }
        }

        // Done, take ownership of new cursor collection
        self.cursors = merged;
    }

    /// Progress single cursor when only one left
    async fn single_cursor(&mut self) {
        assert!(self.cursors.len() == 1);
        let cur = &mut self.cursors[0];
        cur.watch().await;
    }
}

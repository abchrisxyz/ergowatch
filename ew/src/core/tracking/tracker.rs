use tokio::sync::mpsc;
use tracing::info;

use crate::config::PostgresConfig;
use crate::core::node::Node;
use crate::core::store::Store;
use crate::core::tracking::cursor::Cursor;
use crate::core::tracking::messages::TrackingMessage;
use crate::core::types::Head;

/// The capacity of mpsc channels used to communicate tracking events
const CHANNEL_CAPACITY: usize = 8;

pub struct Tracker {
    node: Node,
    cursors: Vec<Cursor>,
    store: Store,
}

impl Tracker {
    pub async fn new(node: Node, pgconf: PostgresConfig) -> Self {
        let mut store = Store::new(pgconf).await;
        if !store.has_genesis_boxes().await {
            let boxes = node.api.utxo_genesis_raw().await.unwrap();
            store.include_genesis_boxes(boxes).await;
        }

        Self {
            node,
            cursors: vec![],
            store,
        }
    }

    pub fn add_cursor<'a>(&mut self, name: String, head: Head) -> mpsc::Receiver<TrackingMessage> {
        // Create new channel
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);

        // New cursor cannot point past tracker's head,
        // so we cap it to tracker's head if needed.
        let max_head = self.store.head();
        let capped_head = if head.height > max_head.height {
            info!(
                "cursor [{}] is ahead of tracker - using tracker's height",
                name
            );
            max_head
        } else {
            head
        };

        // Check for existing cursors at same position
        for cur in &mut self.cursors {
            if cur.is_at(capped_head.height, &capped_head.header_id) {
                cur.txs.push(tx);
                return rx;
            }
        }

        let cur = Cursor {
            name,
            height: capped_head.height,
            header_id: capped_head.header_id,
            node: self.node.clone(),
            txs: vec![tx],
            polling_interval: tokio::time::Duration::from_millis(5000),
        };
        self.cursors.push(cur);
        rx
    }

    pub async fn start(&mut self) {
        tracing::info!("Starting tracker");
        // Ensure genesis boxes have been dispatched
        for cur in &mut self.cursors {
            cur.ensure_genesis_boxes(&mut self.store).await;
        }

        if self.cursors.len() > 1 {
            self.join_cursors().await;
        }
        self.single_cursor().await;
    }

    /// Progresses multiple cursors until they're all at the same position.
    async fn join_cursors(&mut self) {
        loop {
            for cur in &mut self.cursors {
                cur.step(&mut self.store).await;
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
        let mut merged: Vec<Cursor> = vec![self.cursors.remove(0)];

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
        cur.watch(&mut self.store).await;
    }
}

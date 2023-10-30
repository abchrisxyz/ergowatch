use tokio::sync::mpsc;
use tracing::info;

use crate::config::PostgresConfig;
use crate::core::node::Node;
use crate::core::store::Store;
use crate::core::tracking::cursor::Cursor;
use crate::core::tracking::messages::TrackingMessage;
use crate::core::types::Head;
use crate::monitor::MonitorMessage;

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

    /// Get head of tracker's store.
    pub fn head(&self) -> Head {
        self.store.head()
    }

    /// Returns true if `head` is part of tracker's processed main cahin.
    pub async fn contains_head(&self, head: &Head) -> bool {
        // Initial head is always contained but will not be stored,
        // so hande explicitly.
        head.is_initial() || self.store.contains_head(head).await
    }

    pub fn add_cursor<'a>(
        &mut self,
        name: String,
        head: Head,
        monitor_tx: &mpsc::Sender<MonitorMessage>,
    ) -> mpsc::Receiver<TrackingMessage> {
        // Create new channel
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);

        // Workflows may start at a non-zero height and ignore/skip any blocks
        // prior. The tracker's store could be empty or not having reached the
        // workflow's start height yet. Because a cursor cannot point past the
        // tracker's head, we cap it to the current tracker's head if needed.
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

        // If there's an existing cursors at same position we use that one.
        for cur in &mut self.cursors {
            if cur.is_at(capped_head.height, &capped_head.header_id) {
                cur.txs.push(tx);
                return rx;
            }
        }

        // No existing cursors were found, so we make a new one.
        let cur = Cursor {
            name,
            height: capped_head.height,
            header_id: capped_head.header_id,
            node: self.node.clone(),
            txs: vec![tx],
            polling_interval: tokio::time::Duration::from_millis(5000),
            monitor_tx: monitor_tx.clone(),
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
            self.merge_cursors().await;
            if self.cursors.len() == 1 {
                break;
            }
        }
    }

    /// Attempts to merge cursors when at the same height
    async fn merge_cursors(&mut self) {
        // The new collection of cursors with just the first cursor, for now
        let mut merged: Vec<Cursor> = vec![self.cursors.remove(0)];

        // Iterate over remaining cursor in existing collections
        while let Some(cur) = self.cursors.pop() {
            if cur.is_on(&merged[0]) {
                // If encountering an identical cursor, merge it with tip.
                // We only ever merge with tip, so we could miss the opportunity
                // to merge identical cursors behind tip. However, the chances of
                // this occuring are very slim.
                info!("Merging cursors [{}] and [{}]", &merged[0].name, cur.name);
                merged[0].merge(cur).await;
            } else if cur.height > merged[0].height {
                // If next cursor is higher, add to start of new collection
                merged.insert(0, cur);
            } else {
                // Cursor is still behind, just push to back of new collection
                merged.push(cur);
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

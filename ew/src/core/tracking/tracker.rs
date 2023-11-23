use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::info;

use crate::config::PostgresConfig;
use crate::core::node::Node;
use crate::core::store::Store;
use crate::core::tracking::cursor::Cursor;
use crate::core::types::CoreData;
use crate::core::types::Header;
use crate::framework::Event;
use crate::framework::Source;
use crate::monitor::MonitorMessage;

pub struct Tracker {
    node: Node,
    store: Store,
    cursors: Vec<Cursor<CoreData>>,
    monitor_tx: mpsc::Sender<MonitorMessage>,
    // pub polling_interval: tokio::time::Duration,
}

impl Tracker {
    pub async fn new(
        node: Node,
        pgconf: PostgresConfig,
        monitor_tx: mpsc::Sender<MonitorMessage>,
    ) -> Self {
        let mut store = Store::new(pgconf).await;

        // Ensure genesis boxes are included.
        // We do this now, before the tracker can be used by downstream
        // workers, which may request genesis data right away.
        if !store.has_genesis_boxes().await {
            let boxes = node.api.utxo_genesis_raw().await.unwrap();
            store.include_genesis_boxes(boxes).await;
        }

        Self {
            node,
            store,
            cursors: vec![],
            monitor_tx,
            // polling_interval: tokio::time::Duration::from_millis(5000),
        }
    }

    /// Get head of tracker's store.
    pub fn header(&self) -> &Header {
        self.store.header()
    }

    // /// Returns true if `head` is part of tracker's processed main cahin.
    // pub async fn contains_head(&self, head: &Head) -> bool {
    //     // Initial head is always contained but will not be stored,
    //     // so hande explicitly.
    //     head.is_initial() || self.store.contains_head(head).await
    // }

    pub async fn start(&mut self) {
        tracing::info!("Starting tracker");
        // Before starting, reorder cursors by decreasing position.
        self.cursors.sort_by_key(|c| -c.header.height);
        // Rename first cursor to `main` as any other will be merged into it.
        self.cursors[0].id = "main".to_owned();

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
                cur.step(&self.node, &mut self.store).await;
            }
            self.merge_cursors().await;
            if self.cursors.len() == 1 {
                break;
            }
        }
    }

    /// Attempts to merge cursors when at the same height
    async fn merge_cursors(&mut self) {
        // Check if any of the cursors are mergeable.
        let main_header = &self.cursors[0].header;
        if !self.cursors.iter().skip(1).any(|c| c.is_at(main_header)) {
            // Nope, stop here.
            return;
        }

        // The new collection of cursors with just the first cursor, for now
        let mut merged: Vec<Cursor<CoreData>> = vec![self.cursors.remove(0)];

        // Iterate over remaining cursor in existing collections
        while let Some(cur) = self.cursors.pop() {
            if cur.is_on(&merged[0]) {
                // If encountering an identical cursor, merge it with tip.
                // We only ever merge with tip, so we could miss the opportunity
                // to merge identical cursors behind tip. However, the chances of
                // this occuring are very slim.
                merged[0].merge(cur).await;
            } else if cur.header.height > merged[0].header.height {
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
        cur.watch(&self.node, &mut self.store).await;
    }
}

/// Dummy impl to satisfy ErgWorker for now
#[async_trait]
impl Source for Tracker {
    type S = CoreData;

    fn header(&self) -> &Header {
        self.store.header()
    }

    async fn contains_header(&self, header: &Header) -> bool {
        // Initial head is always contained but will not be stored,
        // so hande explicitly.
        header.is_initial() || self.store.contains_header(header).await
    }

    async fn subscribe(
        &mut self,
        header: Header,
        // TODO: cursor name should not be set by caller
        cursor_name: &str,
    ) -> mpsc::Receiver<Event<CoreData>> {
        // Create new channel
        let (tx, rx) = tokio::sync::mpsc::channel(crate::framework::EVENT_CHANNEL_CAPACITY);

        // Workflows may start at a non-zero height and ignore/skip any blocks
        // prior. The tracker's store could be empty or not having reached the
        // workflow's start height yet. Because a cursor cannot point past the
        // tracker's head, we cap it to the current tracker's head if needed.
        let max_header = self.store.header().clone();
        let capped_header = if header.height > max_header.height {
            info!("cursor [{cursor_name}] is ahead of tracker - using tracker's height");
            max_header
        } else {
            header
        };

        // If there's an existing cursor at same position, we use that one.
        for cur in &mut self.cursors {
            if cur.is_at(&capped_header) {
                cur.txs.push(tx);
                return rx;
            }
        }

        // No existing cursors were found, so we make a new one.
        let cur = Cursor {
            id: cursor_name.to_owned(),
            header: capped_header.clone(),
            txs: vec![tx],
            monitor_tx: self.monitor_tx.clone(),
        };
        self.cursors.push(cur);
        rx
    }
}

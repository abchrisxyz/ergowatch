use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use crate::core::node::Node;
use crate::core::node::NodeError;
use crate::core::store::Store;
use crate::core::tracking::messages::TrackingMessage;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Header;
use crate::core::types::HeaderID;
use crate::monitor::MonitorMessage;

pub(super) struct Cursor {
    pub(super) name: String,
    pub(super) height: i32,
    pub(super) header_id: String,
    pub(super) node: Node,
    /// MPSC channel senders
    pub(super) txs: Vec<mpsc::Sender<TrackingMessage>>,
    pub polling_interval: tokio::time::Duration,
    pub monitor_tx: mpsc::Sender<MonitorMessage>,
}

impl Cursor {
    /// Checks if cursor is at given position
    pub fn is_at(&self, height: i32, header_id: &str) -> bool {
        self.height == height && self.header_id == header_id
    }

    /// Checks if cursor is at same position as other
    pub fn is_on(&self, other: &Self) -> bool {
        self.is_at(other.height, &other.header_id)
    }

    /// Merge other cursor into self.
    ///
    /// Other's channels are taken over by self.
    pub fn merge(&mut self, mut other: Self) {
        self.txs.append(&mut other.txs);
    }

    /// Progress the cursor.
    ///
    /// Returns immediately if the next block is not available, if the channel
    /// is full, or after the next block was sent.
    pub async fn step(&mut self, store: &mut Store) {
        match self.fetch_new_headers().await.unwrap() {
            None => (),
            Some(new_headers) => self.process_new_headers(new_headers, store).await,
        }
    }

    /// Watch for new blocks
    ///
    /// Syncs cursor to head and keeps polling for new blocks.
    pub async fn watch(&mut self, store: &mut Store) {
        tracing::debug!("starting watch loop");
        loop {
            let new_headers = self.wait_for_new_blocks().await;
            self.process_new_headers(new_headers, store).await;
        }
    }

    /// Dispatches genesis boxes from the store if needed.
    pub async fn ensure_genesis_boxes(&mut self, store: &mut Store) {
        if self.height > -1 {
            return;
        };
        let boxes = store.get_genesis_boxes().await;
        for tx in &self.txs {
            tx.send(TrackingMessage::Genesis(boxes.clone()))
                .await
                .unwrap()
        }
        let head = Head::genesis();
        self.height = head.height;
        self.header_id = head.header_id;
    }

    async fn process_new_headers(&mut self, new_headers: Vec<Header>, store: &mut Store) {
        tracing::debug!(
            "[{}] processing {} new headers",
            self.name,
            new_headers.len()
        );
        for new_header in new_headers {
            if new_header.height == self.height {
                // Different block at same height, last included block is
                // not part of main chain anymore, so roll back and start over.
                tracing::warn!(">>>>>>>>>   same height");
                self.roll_back(store).await;
            }
            assert_eq!(new_header.height, self.height + 1);
            if new_header.parent_id != self.header_id {
                // New block is not a child of current last block.
                tracing::warn!(">>>>>>>>>   not a child");
                self.roll_back(store).await;
                break;
            } else {
                self.include(&new_header.id, store).await;
            }
        }
    }

    /// Return a header id for next height, once available.
    async fn wait_for_new_blocks(&self) -> Vec<Header> {
        loop {
            match self.fetch_new_headers().await {
                Ok(res) => match res {
                    Some(headers) => {
                        return headers;
                    }
                    None => {
                        tokio::time::sleep(self.polling_interval).await;
                    }
                },
                Err(e) => {
                    tracing::warn!("{}", e);
                    tokio::time::sleep(self.polling_interval).await;
                }
            }
        }
    }

    /// Return header id's for next few heights, if any.
    //TODO: Can probably avoid deserializing whole headers by using dedicated type with relevant fields only.
    async fn fetch_new_headers(&self) -> Result<Option<Vec<Header>>, NodeError> {
        let fr = self.height;
        let to = fr + 10;
        let headers: Vec<Header> = self
            .node
            .api
            .chainslice(fr, to)
            .await?
            .into_iter()
            .map(|node_header| Header::from(node_header))
            .collect();

        match headers.len() {
            // one header
            1 => {
                if headers[0].id == self.header_id {
                    Ok(None)
                } else {
                    Ok(Some(headers))
                }
            }
            // no headers
            0 => {
                assert!(headers.is_empty());
                panic!("Got empty chainslice");
            }
            // more than 1 header
            _ => Ok(Some(headers)),
        }
    }

    /// Submit block for inclusion and update cursor
    async fn include(&mut self, header_id: &HeaderID, store: &mut Store) {
        info!(
            "[{}] including block {} for height {}",
            self.name,
            header_id,
            self.height + 1
        );
        let block_json_string: String = self.node.api.block_raw(header_id).await.unwrap();
        let core_data: CoreData = store.process(self.height + 1, block_json_string).await;
        let payload = Arc::new(core_data);
        // Broadcast inclusion of next block
        for tx in &self.txs {
            tx.send(TrackingMessage::Include(payload.clone()))
                .await
                .unwrap();
        }
        // Update position
        self.height += 1;
        self.header_id = header_id.clone();
        self.monitor_tx
            .send(MonitorMessage::CoreUpdate((self.height)))
            .await
            .unwrap();
    }

    /// Submit block for roll back and update cursor
    async fn roll_back(&mut self, store: &mut Store) {
        info!(
            "[{}] Rolling back block {} for height {}",
            self.name, self.header_id, self.height
        );
        // Retrieve data of block to be rolled back
        let curr = Head {
            height: self.height,
            header_id: self.header_id.clone(),
        };
        let prev: Head = store.roll_back(&curr).await;
        // Broadcast roll back
        for tx in &self.txs {
            tx.send(TrackingMessage::Rollback(curr.height))
                .await
                .unwrap();
        }
        // Wind back the cursor
        self.height = prev.height;
        self.header_id = prev.header_id;
    }
}

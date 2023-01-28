use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

use crate::node::models::Header;
use crate::node::models::HeaderID;
use crate::node::Node;
use crate::node::NodeError;
use crate::render::RenderedBlock;
use crate::track::messages::TrackingMessage;

pub(super) struct Cursor {
    pub(super) name: String,
    pub(super) height: i32,
    pub(super) header_id: String,
    pub(super) node: Node,
    /// MPSC channel senders
    pub(super) txs: Vec<mpsc::Sender<TrackingMessage>>,
    pub polling_interval: tokio::time::Duration,
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
    pub async fn step(&mut self) {
        match self.fetch_new_headers().await.unwrap() {
            None => (),
            Some(new_headers) => self.process_new_headers(new_headers).await,
        }
    }

    pub async fn watch(&mut self) {
        loop {
            let new_headers = self.wait_for_new_blocks().await;
            self.process_new_headers(new_headers).await;
        }
    }

    async fn process_new_headers(&mut self, new_headers: Vec<Header>) {
        for new_header in new_headers {
            if new_header.height == self.height {
                // Different block at same height, last included block is
                // not part of main chain anymore, so roll back and start over.
                tracing::warn!(">>>>>>>>>   same height");
                self.roll_back().await;
            }
            assert_eq!(new_header.height, self.height + 1);
            if new_header.parent_id != self.header_id {
                // New block is not a child of current last block.
                tracing::warn!(">>>>>>>>>   not a child");
                self.roll_back().await;
                break;
            } else {
                self.include(&new_header.id).await;
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

    /// Return a header id for next height, if any.
    async fn fetch_new_headers(&self) -> Result<Option<Vec<Header>>, NodeError> {
        let fr = self.height;
        let to = fr + 10;
        let headers = self.node.api.chainslice(fr, to).await?;
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
    async fn include(&mut self, header_id: &HeaderID) {
        info!(
            "[{}] Including block {} for height {}",
            self.name,
            header_id,
            self.height + 1
        );
        let next_block: RenderedBlock = self.node.api.block(header_id).await.unwrap().into();
        let wrapped_block = Arc::new(next_block);
        // Broadcast inclusion of next block
        for tx in &self.txs {
            tx.send(TrackingMessage::Include(wrapped_block.clone()))
                .await
                .unwrap();
            // Progress the cursor
        }
        // Update position
        self.height += 1;
        self.header_id = header_id.to_string();
    }

    /// Submit block for roll back and update cursor
    async fn roll_back(&mut self) {
        info!(
            "[{}] Rolling back block {} for height {}",
            self.name, self.header_id, self.height
        );
        // Retrieve data of block to be rolled back
        let block: RenderedBlock = self.node.api.block(&self.header_id).await.unwrap().into();
        let header_id = block.header.parent_id.to_string();
        let wrapped_block = Arc::new(block);
        // Broadcast roll back
        for tx in &self.txs {
            tx.send(TrackingMessage::Rollback(wrapped_block.clone()))
                // .send(TrackingEvent::Rollback(block.into()))
                .await
                .unwrap();
        }
        // Wind back the cursor
        self.height -= 1;
        self.header_id = header_id;
    }
}

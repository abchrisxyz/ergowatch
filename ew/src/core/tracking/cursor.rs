use crate::core::node::Node;
use crate::core::node::NodeError;
use crate::core::store::Store;
use crate::core::types::Block;
use crate::core::types::CoreData;
use crate::core::types::Head;
use crate::core::types::Header;
pub use crate::framework::Cursor;
use crate::framework::StampedData;

impl Cursor<CoreData> {
    /// Progress the cursor.
    ///
    /// Returns immediately if the next block is not available, if the channel
    /// is full, or after the next block was sent.
    pub(super) async fn step(&mut self, node: &Node, store: &mut Store) {
        match self.fetch_new_headers(node).await.unwrap() {
            None => (),
            Some(new_headers) => self.process_new_headers(new_headers, node, store).await,
        }
    }

    /// Watch for new blocks
    ///
    /// Syncs cursor to head and keeps polling for new blocks.
    pub(super) async fn watch(&mut self, node: &Node, store: &mut Store) {
        tracing::debug!("[{}] starting watch loop", self.id);
        loop {
            let new_headers = self.wait_for_new_blocks(node).await;
            self.process_new_headers(new_headers, node, store).await;
        }
    }

    /// Dispatches genesis boxes from the store if needed.
    pub(super) async fn ensure_genesis_boxes(&mut self, store: &mut Store) {
        if self.head.height > -1 {
            return;
        };
        let boxes = store.get_genesis_boxes().await;
        let fake_block = Block::from_genesis_boxes(boxes);
        let data = StampedData {
            height: fake_block.header.height,
            header_id: fake_block.header.id.clone(),
            parent_id: "".to_owned(),
            data: CoreData { block: fake_block },
        };
        self.include(data).await;
    }

    async fn process_new_headers(
        &mut self,
        new_headers: Vec<Header>,
        node: &Node,
        store: &mut Store,
    ) {
        tracing::debug!("[{}] processing {} new headers", self.id, new_headers.len());
        for new_header in new_headers {
            if new_header.height == self.head.height {
                // Different block at same height, last included block is
                // not part of main chain anymore, so roll back and start over.
                tracing::warn!("last included block is not part of main chain anymore");
                let prev: Head = store.roll_back(&self.head).await;
                self.roll_back(prev).await;
                break;
            }
            assert_eq!(new_header.height, self.head.height + 1);
            if new_header.parent_id != self.head.header_id {
                // New block is not a child of current last block.
                tracing::warn!("new block is not a child of current last block");
                let prev: Head = store.roll_back(&self.head).await;
                self.roll_back(prev).await;
                break;
            } else {
                let block = match node.api.block(&new_header.id).await {
                    Ok(b) => b,
                    Err(NodeError::API404Notfound(url)) => {
                        // Block wasn't found. This can happen when at the tip of
                        // the chain and the header is in but the corresponding
                        // block not yet.
                        // Pause for a short time, then break to try again.
                        tracing::warn!("404 for {}", { url });
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        break;
                    }
                    Err(other_node_error) => {
                        panic!("{:?}", other_node_error)
                    }
                };

                assert_eq!(block.header.height, self.head.height + 1);
                let core_data = store.process(block).await;

                let data = StampedData {
                    height: core_data.block.header.height,
                    header_id: core_data.block.header.id.clone(),
                    parent_id: core_data.block.header.parent_id.clone(),
                    data: core_data,
                };

                self.include(data).await;
            }
        }
    }

    /// Return a header id for next height, once available.
    async fn wait_for_new_blocks(&self, node: &Node) -> Vec<Header> {
        let polling_interval = tokio::time::Duration::from_millis(5000);
        loop {
            match self.fetch_new_headers(node).await {
                Ok(res) => match res {
                    Some(headers) => {
                        return headers;
                    }
                    None => {
                        tokio::time::sleep(polling_interval).await;
                    }
                },
                Err(e) => {
                    tracing::warn!("{}", e);
                    tokio::time::sleep(polling_interval).await;
                }
            }
        }
    }

    /// Return header id's for next few heights, if any.
    //TODO: Can probably avoid deserializing whole headers by using dedicated type with relevant fields only.
    pub(super) async fn fetch_new_headers(
        &self,
        node: &Node,
    ) -> Result<Option<Vec<Header>>, NodeError> {
        let fr = self.head.height;
        let to = fr + 100;
        let headers: Vec<Header> = node
            .api
            .chainslice(fr, to)
            .await?
            .into_iter()
            .map(|node_header| Header::from(node_header))
            .collect();

        match headers.len() {
            // one header
            1 => {
                if headers[0].id == self.head.header_id {
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
}

pub mod models;

use ergotree_ir::chain::ergo_box::ErgoBox;
use log::debug;
use log::info;
use log::warn;
use reqwest;

use models::Block;
use models::HeaderID;
use models::Height;
use models::NodeInfo;

pub struct Node {
    url: String,
}

impl Node {
    pub fn new(url: String) -> Self {
        Node { url: url }
    }

    pub fn get_height(&self) -> Result<Height, reqwest::Error> {
        let url = format!("{}/info", self.url);
        let node_info: NodeInfo = reqwest::blocking::get(url)?.json()?;
        Ok(node_info.full_height)
    }

    pub fn get_genesis_blocks(&self) -> Result<Vec<ErgoBox>, reqwest::Error> {
        let url = format!("{}/utxo/genesis", self.url);
        let boxes: Vec<ErgoBox> = reqwest::blocking::get(url)?.json()?;
        Ok(boxes)
    }

    fn get_blocks_at(&self, height: Height) -> Result<Vec<HeaderID>, reqwest::Error> {
        let url = format!("{}/blocks/at/{}", self.url, height);
        let json: Vec<String> = reqwest::blocking::get(url)?.json()?;
        Ok(json.to_owned())
    }

    /// Get full block from `header_id`
    pub fn get_block(&self, header_id: &HeaderID) -> Option<Block> {
        let url = format!("{}/blocks/{}", self.url, header_id);
        let res = match reqwest::blocking::get(url) {
            Ok(res) => res,
            Err(e) => {
                warn!("Failed requesting block from node.");
                warn!("{}", e);
                return None;
            }
        };
        match res.json() {
            Ok(json) => Some(json),
            Err(e) => {
                warn!("Failed deserializing block data");
                warn!("{}", e);
                None
            }
        }
    }

    pub fn get_main_chain_block_at(&self, height: Height) -> Option<Block> {
        let header_ids = self.get_blocks_at(height).unwrap();
        if header_ids.is_empty() {
            return None;
        }
        let block = match header_ids.len() {
            1 => self.get_block(&header_ids[0]).unwrap(),
            _ => {
                info!(
                    "Multiple candidates ({}) for height {}",
                    header_ids.len(),
                    height
                );
                // Attempt to identify main chain block.
                match self.resolve(height, &header_ids) {
                    Some(idx) => self.get_block(&header_ids[idx]).unwrap(),
                    None => {
                        // If not possible to tell blocks appart yet, then pick first one that is valid.
                        // If it turns out not to be the main chain, it'll get rolled back eventually.
                        info!("Picking first valid block");
                        match header_ids.iter().find_map(|hid| self.get_block(&hid)) {
                            Some(block) => block,
                            None => panic!("No valid blocks available out of {}", header_ids.len()),
                        }
                    }
                }
            }
        };
        Some(block)
    }

    /// Returns the index of the header that belongs to the main chain
    fn resolve<'a>(&self, height: Height, header_ids: &'a [HeaderID]) -> Option<usize> {
        // Look ahead to determine which block is on the main chain
        let next_height = height + 1;
        info!(
            "Looking ahead for main chain child at height {}",
            next_height
        );
        let next_header_ids = self.get_blocks_at(next_height).unwrap();

        if next_header_ids.is_empty() {
            // We can't tell blocks appart yet.
            info!("Next block is not available yet");
            return None;
        } else if next_header_ids.len() == 1 {
            debug!("Found main chain child at height {}", next_height);
        } else {
            let index = self.resolve(next_height, &next_header_ids);
            if let Some(idx) = index {
                debug!(
                    "Using header {} at index {} for height {}",
                    &header_ids[idx], idx, height
                );
            }
        }

        // At this point next_header_ids is guaranteed to have exactly 1 element.
        let next_block = self.get_block(&next_header_ids[0]).unwrap();
        let index = header_ids
            .iter()
            .position(|h| h == &next_block.header.parent_id)
            .unwrap();
        Some(index)
    }
}

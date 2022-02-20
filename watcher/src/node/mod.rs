pub mod models;

use log::debug;
use log::info;
use reqwest;

use models::Block;
use models::HeaderID;
use models::Height;
use models::NodeInfo;
use models::Output;

pub struct Node {
    url: String,
}

impl Node {
    pub fn new(url: String) -> Self {
        Node { url: url }
    }

    pub fn get_height(&self) -> Result<Height, reqwest::Error> {
        let url = format!("{}/info", self.url);
        debug!("URL: {}", url);
        let node_info: NodeInfo = reqwest::blocking::get(url)?.json()?;
        Ok(node_info.full_height)
    }

    pub fn get_genesis_blocks(&self) -> Result<Vec<Output>, reqwest::Error> {
        let url = format!("{}/utxo/genesis", self.url);
        let boxes: Vec<Output> = reqwest::blocking::get(url)?.json()?;
        Ok(boxes)
    }

    fn get_blocks_at(&self, height: Height) -> Result<Vec<HeaderID>, reqwest::Error> {
        let url = format!("{}/blocks/at/{}", self.url, height);
        debug!("URL: {}", url);
        let json: Vec<String> = reqwest::blocking::get(url)?.json()?;
        Ok(json.to_owned())
    }

    pub fn get_block(&self, header_id: &HeaderID) -> Result<Block, reqwest::Error> {
        let url = format!("{}/blocks/{}", self.url, header_id);
        debug!("URL: {}", url);
        let json: Block = reqwest::blocking::get(url)?.json()?;
        Ok(json)
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
                self.get_block(&header_ids[self.resolve(height, &header_ids)])
                    .unwrap()
            }
        };
        Some(block)
    }

    /// Returns the index of the header that belongs to the main chain
    fn resolve<'a>(&self, height: Height, header_ids: &'a [HeaderID]) -> usize {
        // Look ahead to determine which block is on the main chain
        let next_height = height + 1;
        info!(
            "Looking ahead for main chain child at height {}",
            next_height
        );
        let next_header_ids = self.get_blocks_at(next_height).unwrap();

        if next_header_ids.is_empty() {
            // We can't tell blocks appart yet, take first one for now.
            // If it turns out no to be the main chain, it'll get rolled back eventually.
            info!("Next block is not available yet - assuming first header is main chain for now");
            return 0;
        } else if next_header_ids.len() == 1 {
            debug!("Found main chain child at height {}", next_height);
        } else {
            let index = self.resolve(next_height, &next_header_ids);
            debug!(
                "Using header {} at index {} for height {}",
                &header_ids[index], index, height
            );
        }

        let next_block = self.get_block(&next_header_ids[0]).unwrap();
        return header_ids
            .iter()
            .position(|h| h == &next_block.header.parent_id)
            .unwrap();
    }
}

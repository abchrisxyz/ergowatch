mod block_1;
mod block_2;
mod block_3;
mod block_4;
mod block_5;
mod genesis;
use ew::core::testing::NodeBlock;
use ew::core::types::Header;
use serde_json;

pub use block_1::BLOCK_1;
pub use block_2::BLOCK_2;
pub use block_3::BLOCK_3;
pub use block_3::BLOCK_3BIS;
pub use block_4::BLOCK_4;
pub use block_5::BLOCK_5;
pub use genesis::GENESIS_BOXES;

// pub type TestBlock = &'static str;
pub struct TestBlock {
    /// Json string as returned by node API
    str: &'static str,
    /// Deserialized instance
    block: NodeBlock,
}

impl TestBlock {
    pub fn new(str: &'static str) -> Self {
        Self {
            str,
            block: serde_json::from_str(str).unwrap(),
        }
    }

    pub fn from_id(id: &str) -> Self {
        let raw_id = id.strip_suffix("*").unwrap_or(id);
        let str = match raw_id {
            "1" => BLOCK_1,
            "2" => BLOCK_2,
            "3" => BLOCK_3,
            "3bis" => BLOCK_3BIS,
            "4" => BLOCK_4,
            "5" => BLOCK_5,
            _ => panic!("Unknown TestBlock id: {}", raw_id),
        };
        Self::new(str)
    }

    /// Returns block's header id
    pub fn header_id(&self) -> &str {
        &self.block.header.id
    }

    /// Returns a Header for current block
    #[allow(dead_code)] // Not used by all tests
    pub fn header(&self) -> Header {
        Header {
            height: self.block.header.height,
            timestamp: self.block.header.timestamp,
            header_id: self.block.header.id.clone(),
            parent_id: self.block.header.parent_id.clone(),
        }
    }

    /// Returns block's height
    pub fn height(&self) -> i32 {
        self.block.header.height
    }

    /// Returns block as json object
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::from_str(self.str).unwrap()
    }
}

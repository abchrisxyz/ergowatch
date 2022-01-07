use crate::node;

pub type Height = u32;

pub struct Header {
    pub height: u32,
    pub id: String,
    pub parent_id: String,
    pub timestamp: u64,
}

impl From<node::models::Block> for Header {
    fn from(block: node::models::Block) -> Self {
        Header {
            height: block.header.height,
            id: block.header.id,
            parent_id: block.header.parent_id,
            timestamp: block.header.timestamp,
        }
    }
}

impl From<&node::models::Block> for Header {
    fn from(block: &node::models::Block) -> Self {
        Header {
            height: block.header.height,
            id: block.header.id.clone(),
            parent_id: block.header.parent_id.clone(),
            timestamp: block.header.timestamp,
        }
    }
}

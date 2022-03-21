use crate::node;

// pub type Height = u32;
// pub type HeaderID = String;

/// Represents last block synced to db
pub struct Head {
    pub height: u32,
    pub header_id: String,
}

#[derive(Debug, PartialEq)]
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

// ToDo use &str instread of String (node Block will always outlive Transaction)
#[derive(Debug, PartialEq)]
pub struct Transaction {
    pub id: String,
    pub header_id: String,
    pub height: u32,
    pub index: u32,
}

//! # core
//!
//! Process blocks into core tables data.

use log::info;

use super::Unit;
use crate::db;
use crate::node::models::Block;
use crate::types::Header;
// use crate::types::Height;

pub struct CoreUnit {
    pub last_height: u32,
    pub last_header_id: String,
}

impl Unit for CoreUnit {
    fn ingest(self: &mut Self, block: &Block) -> () {
        assert_eq!(self.last_header_id, block.header.parent_id);
        let header = Header::from(block);
        db::core::insert_header(&header).unwrap();
        info!("Added header {} for height {}", header.id, header.height);
        self.last_height = header.height;
        self.last_header_id = header.id;
    }

    fn rollback(self: &Self, block: &Block) -> () {
        let header = Header::from(block);
        db::core::delete_header(&header).unwrap();
        info!("Deleted header {} for height {}", header.id, header.height);
    }

    // fn poll() -> Height {
    //     todo!()
    // }
}

impl CoreUnit {
    pub fn new_genesis() -> CoreUnit {
        CoreUnit {
            last_height: 0,
            last_header_id: String::from(
                "0000000000000000000000000000000000000000000000000000000000000000",
            ),
        }
    }

    pub fn new() -> CoreUnit {
        let head = db::core::get_last_header().unwrap();
        CoreUnit {
            last_height: head.height,
            last_header_id: head.id,
        }
    }

    // pub fn last_height(self: &Self) -> u32 {
    //     self.last_height
    // }
}

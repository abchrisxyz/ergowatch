use super::Unit;
use crate::db;
use crate::node::models::Block;
use crate::types::Header;
// use crate::types::Height;
use log::info;

/// Main unit used to track db sync state.
pub struct HeaderUnit;

impl Unit for HeaderUnit {
    fn ingest(self: &Self, block: &Block) -> () {
        let header = Header::from(block);
        db::insert_header(&header).unwrap();
        info!("Added header {} for height {}", header.id, header.height);
    }

    fn rollback(self: &Self, block: &Block) -> () {
        let header = Header::from(block);
        db::delete_header(&header).unwrap();
        info!("Deleted header {} for height {}", header.id, header.height);
    }

    // fn poll() -> Height {
    //     todo!()
    // }
}

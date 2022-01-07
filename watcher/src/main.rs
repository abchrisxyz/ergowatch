mod db;
mod node;
mod types;
mod units;

use log::debug;
use log::error;
use log::info;
use std::{thread, time};
use crate::units::Unit;

const POLL_INTERVAL_SECONDS: u64 = 5;

/// Process genesis block if needed.
fn init() -> () {
    let db_height = match db::get_height() {
        Ok(h) => h,
        Err(e) => {
            error!("{}", e);
            panic!("Failed retrieving height from db");
        }
    };

    if db_height > 0 {
        // Already initialized, do nothing.
        return;
    }

    info!("Adding genesis block");
    let header_id = node::api::get_block_at(1).unwrap();
    let header = types::Header::from(node::api::get_block(header_id).unwrap());
    db::insert_header(&header).unwrap();
}



fn main() {
    env_logger::init();
    info!("Starting Ergo Watcher");

    init();

    let hu = units::headers::HeaderUnit;
    // let hu2 = units::headers::HeaderUnit;

    let units: [&dyn Unit; 1] = [&hu];


    // Header of head block in db
    let mut head = db::get_last_header().unwrap();
    info!("DB head: {}", head.id);
    loop {
        let node_height = match node::api::get_height() {
            Ok(h) => h,
            Err(e) => {
                error!("Failed retrieving height from node: {}", e);
                continue;
            }
        };

        if node_height <= head.height {
            debug!("No new heights - waiting {} seconds", POLL_INTERVAL_SECONDS);
            thread::sleep(time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            continue;
        }

        for height in head.height + 1..node_height + 1 {
            let header_id = node::api::get_block_at(height).unwrap();
            let block = node::api::get_block(header_id).unwrap();
            assert_eq!(head.id, block.header.parent_id);
            
            units.iter().for_each(|u| u.ingest(&block));

            head = types::Header::from(block);
        }
    }
}

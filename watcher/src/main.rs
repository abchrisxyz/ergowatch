mod db;
mod node;
mod types;
mod units;

use crate::units::Unit;
use log::debug;
use log::error;
use log::info;
use std::{thread, time};

const POLL_INTERVAL_SECONDS: u64 = 5;

// Process genesis block if needed.
fn init() -> () {
    let db_height = match db::core::get_height() {
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
    db::core::insert_header(&header).unwrap();
}

fn main() {
    env_logger::init();
    info!("Starting Ergo Watcher");

    init();

    // let core = units::core::CoreUnit::new();

    // let units: [&dyn Unit; 1] = [&core];
    let mut units = vec![Box::new(units::core::CoreUnit::new())];

    info!("DB core is at height: {}", units[0].last_height);

    // core.last_height += 1;

    loop {
        let node_height = match node::api::get_height() {
            Ok(h) => h,
            Err(e) => {
                error!("Failed retrieving height from node: {}", e);
                continue;
            }
        };

        if node_height <= units[0].last_height {
            debug!("No new heights - waiting {} seconds", POLL_INTERVAL_SECONDS);
            thread::sleep(time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            continue;
        }

        while units[0].last_height < node_height {
            let next_height = units[0].last_height + 1;
            // Fetch next block from node
            let header_id = node::api::get_block_at(next_height).unwrap();
            let block = node::api::get_block(header_id).unwrap();

            if block.header.parent_id == units[0].last_header_id {
                // Process block
                info!(
                    "Including block {} for height {}",
                    block.header.id, block.header.height
                );
                units.iter_mut().for_each(|u| u.ingest(&block));
            } else {
                // Rollback block
                // ToDo retrieve last blocl to rollback...
                // This requires all data processed in other units to be available from core unit
                // to rebuild a block.
                // info!(
                //     "Rolling back block {} for height {}",
                //     block.header.id, block.header.height
                // );
                // units.iter().rev().for_each(|u| u.rollback(&block));
            }
        }
    }
}

mod db;
mod node;
mod types;
mod units;

use log::debug;
use log::info;
use std::{thread, time};

// const DB_VERSION: i32 = 1;
const POLL_INTERVAL_SECONDS: u64 = 5;

fn main() {
    env_logger::init();
    info!("Starting Ergo Watcher");

    // ToDo: check db version

    let mut head = db::get_head().unwrap();
    info!("Database is currently at block {}", head.height);

    let core = units::core::CoreUnit {};

    let node = node::Node::new();

    loop {
        let node_height = node.get_height().unwrap();

        if node_height <= head.height {
            debug!("No new blocks - waiting {} seconds", POLL_INTERVAL_SECONDS);
            thread::sleep(time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            continue;
        }

        while head.height < node_height {
            let next_height = head.height + 1;
            // Fetch next block from node
            let header_id = node.get_block_at(next_height).unwrap();
            let block = node.get_block(header_id).unwrap();

            if block.header.parent_id == head.header_id {
                // Process block
                info!(
                    "Including block {} for height {}",
                    block.header.id, block.header.height
                );

                // Collect statements
                let statements = core.prep(&block);

                // Execute statements in single transaction
                db::execute_in_transaction(statements).unwrap();

                // Move head to latest block
                head.height = next_height;
                head.header_id = block.header.id;
            } else {
                panic!("Rollback is not implemented yet");
                // Rollback block
                // ToDo retrieve last block to rollback...
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

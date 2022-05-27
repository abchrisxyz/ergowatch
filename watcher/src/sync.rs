use log::debug;
use log::error;
use log::info;
use log::warn;
use std::{thread, time};

use crate::parsing::BlockData;
use crate::session::Session;

/// Sync db and track node in infinite loop
pub fn sync_and_track(session: &mut Session) -> Result<(), &'static str> {
    info!("Synchronizing with node");

    loop {
        let mut node_height = get_node_height_blocking(session);

        if node_height <= session.head.height {
            // Is it time to start a repair event?
            if (session.head.height) % session.repair_interval == 0 {
                info!("Starting repair session");
                let max_height = session.head.height - session.repair_offset;
                session.db.start_repair_event(max_height as i32);
            }

            // -x --exit option, exit when synced
            if session.exit_when_synced {
                session.db.wait_for_repairs();
                info!("Done syncing, exiting now");
                return Ok(());
            }
            info!("Database is synced - waiting for next block");
            node_height = weight_for_next_block(&session);
        }

        sync_to_height(session, node_height).unwrap();
    }
}

/// Sync db to given height
fn sync_to_height(session: &mut Session, node_height: u32) -> Result<(), &'static str> {
    while session.head.height < node_height {
        let next_height = session.head.height + 1;
        // Fetch next block from node
        let block = session.node.get_main_chain_block_at(next_height).unwrap();

        if block.header.parent_id == session.head.header_id {
            info!(
                "Including block {} for height {}",
                block.header.id, block.header.height
            );

            let prepped_block = BlockData::new(&block);
            include_block(session, &prepped_block);

            // Move head to latest block
            session.head.height = next_height;
            session.head.header_id = block.header.id;
        } else {
            // New block is not a child of last processed block, need to rollback.
            warn!(
                "Rolling back block {} at height {}",
                session.head.header_id, session.head.height
            );

            // Rollbacks may rely on database constraints to propagate.
            // So prevent any rollbacks if constraints haven't been set.
            if !session.allow_rollbacks {
                warn!("Preventing a rollback on an unconstrained database.");
                return Err("Preventing a rollback on an unconstrained database.");
            }

            // Retrieve processed block from node
            let block = session.node.get_block(&session.head.header_id).unwrap();

            // Rollback db
            let prepped_block = BlockData::new(&block);
            rollback_block(session, &prepped_block);

            // Move head to previous block
            session.head.height = block.header.height - 1;
            session.head.header_id = block.header.parent_id;
        }
    }
    Ok(())
}

/// Add genesis boxes to core tables
fn include_genesis_boxes(session: &mut Session) -> Result<(), &'static str> {
    let boxes = match session.node.get_genesis_blocks() {
        Ok(boxes) => boxes,
        Err(e) => {
            error!("{}", e);
            return Err("Failed to retrieve genesis boxes from node");
        }
    };
    session.db.include_genesis_boxes(boxes).unwrap();

    // Resync cache now that db got modified
    // Not needed currently, but calling anyway in case we rely on the cache
    // for core tables at some point.
    session.db.load_cache();

    Ok(())
}

/// Process block data into database
fn include_block(session: &mut Session, block: &BlockData) {
    session.db.include_block(block, &mut session.cache).unwrap();
}

/// Discard block data from database
fn rollback_block(session: &mut Session, block: &BlockData) {
    if session.db.is_repairing_height(block.height) {
        session.db.abort_repairs();
    }
    session
        .db
        .rollback_block(block, &mut session.cache)
        .unwrap();
}

/// Get latest block height from node.
/// Keeps trying until node is responsive.
fn get_node_height_blocking(session: &Session) -> u32 {
    loop {
        match session.node.get_height() {
            Ok(h) => return h,
            Err(e) => {
                error!("{}", e);
                info!("Retrying in {} seconds", session.poll_interval);
                thread::sleep(time::Duration::from_secs(session.poll_interval));
                continue;
            }
        };
    }
}

/// Returns new height when available
fn weight_for_next_block(session: &Session) -> u32 {
    loop {
        let h = get_node_height_blocking(session);
        if h != session.head.height {
            return h;
        }
        debug!("No new blocks - waiting {} seconds", session.poll_interval);
        thread::sleep(time::Duration::from_secs(session.poll_interval));
    }
}

pub mod bootstrap {
    use super::get_node_height_blocking;
    use crate::parsing::BlockData;
    use crate::session::Session;
    use log::info;

    pub fn run(session: &mut Session) -> Result<(), &'static str> {
        if !session.db.has_constraints().unwrap() {
            // Bootstrap core
            sync_core(session)?;
            session.db.apply_core_constraints().unwrap();
            session.allow_rollbacks = true;
            info!("Core bootstrapping completed");
        }
        session.db.bootstrap_derived_schemas().unwrap();
        Ok(())
    }

    /// Sync core tables only.
    fn sync_core(session: &mut Session) -> Result<(), &'static str> {
        info!("Bootstrapping step 1/2 - syncing core tables");

        if !session.db.has_genesis_boxes() {
            super::include_genesis_boxes(session)?;
        }

        loop {
            let node_height = get_node_height_blocking(session);
            if node_height <= session.head.height {
                break;
            }
            sync_core_to_height(session, node_height)?;
        }
        info!("Core sync completed");
        Ok(())
    }

    /// Sync db core to given height
    ///
    /// No rollback support.
    fn sync_core_to_height(session: &mut Session, node_height: u32) -> Result<(), &'static str> {
        while session.head.height < node_height {
            let next_height = session.head.height + 1;
            // Fetch next block from node
            let block = session.node.get_main_chain_block_at(next_height).unwrap();
            info!(
                "Bootstrapping block {} for height {}",
                block.header.id, block.header.height
            );

            let prepped_block = BlockData::new(&block);
            include_block(session, &prepped_block);

            // Move head to latest block
            session.head.height = next_height;
            session.head.header_id = block.header.id;
        }
        Ok(())
    }

    /// Process block data into database.
    /// Core tables only.
    fn include_block(session: &Session, block: &BlockData) {
        session.db.include_block_core_only(block).unwrap();
    }
}

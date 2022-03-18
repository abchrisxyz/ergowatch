use log::debug;
use log::error;
use log::info;
use log::warn;
use std::{thread, time};

use crate::db;
use crate::parsing::BlockData;
use crate::session::Session;

// TODO: move this to config
const POLL_INTERVAL_SECONDS: u64 = 5;

/// Sync db and track node in infinite loop
pub fn sync_and_track(session: &mut Session) -> Result<(), &'static str> {
    info!("Synchronizing with node");
    loop {
        let node_height = get_node_height_blocking(session);

        if node_height <= session.head.height {
            if session.exit_when_synced {
                debug!("Done syncing, exiting now");
                return Ok(());
            }
            debug!("No new blocks - waiting {} seconds", POLL_INTERVAL_SECONDS);
            thread::sleep(time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            continue;
        }

        sync_to_height(session, node_height).unwrap();

        info!("Database is synced - waiting for next block");
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
            if !session.db_constraints_set {
                warn!("Preventing a rollback on an unconstrained database.");
                warn!("Rollbacks may rely on database constraints to propagate.");
                warn!("Please set the database contraints defined in `constraints.sql`.");
                return Err("Preventing a rollback on an unconstrained database.");
            }

            // Retrieve processed block from node
            let block = session.node.get_block(&session.head.header_id).unwrap();

            // Collect rollback statements, in reverse order
            let prepped_block = BlockData::new(&block);
            rollback_block(session, &prepped_block);

            // Move head to previous block
            session.head.height = block.header.height - 1;
            session.head.header_id = block.header.parent_id;
        }
    }
    Ok(())
}

/// Add genesis boxes to database
pub fn include_genesis_boxes(session: &Session) -> Result<(), &'static str> {
    info!("Retrieving genesis boxes");
    let boxes = match session.node.get_genesis_blocks() {
        Ok(boxes) => boxes,
        Err(e) => {
            error!("{}", e);
            return Err("Failed to retrieve genesis boxes from node");
        }
    };
    let sql_statements = db::core::genesis::prep(boxes);
    session.db.execute_in_transaction(sql_statements).unwrap();
    Ok(())
}

/// Process block data into database
fn include_block(session: &Session, block: &BlockData) {
    // Prepare statements
    let mut sql_statements = db::core::prep(block);
    sql_statements.append(&mut db::unspent::prep(block));
    sql_statements.append(&mut db::balances::prep(block));

    // Execute statements in single transaction
    session.db.execute_in_transaction(sql_statements).unwrap();
}

/// Discard block data from database
fn rollback_block(session: &Session, block: &BlockData) {
    // Collect rollback statements, in reverse order
    let mut sql_statements: Vec<db::SQLStatement> = vec![];
    sql_statements.append(&mut db::balances::prep_rollback(block));
    sql_statements.append(&mut db::unspent::prep_rollback(block));
    sql_statements.append(&mut db::core::prep_rollback(block));

    // Execute statements in single transaction
    session.db.execute_in_transaction(sql_statements).unwrap();
}

/// Get latest block height from node.
/// Keeps trying until node is responsive.
fn get_node_height_blocking(session: &Session) -> u32 {
    loop {
        match session.node.get_height() {
            Ok(h) => return h,
            Err(e) => {
                error!("{}", e);
                info!("Retrying in {} seconds", POLL_INTERVAL_SECONDS);
                thread::sleep(time::Duration::from_secs(POLL_INTERVAL_SECONDS));
                continue;
            }
        };
    }
}

pub mod bootstrap {
    use super::get_node_height_blocking;
    use crate::db;
    use crate::parsing::BlockData;
    use crate::session::Session;
    use log::info;

    /// Sync core tables only.
    pub fn sync_core(session: &mut Session) -> Result<(), &'static str> {
        info!("Bootstrapping step 1/2 - syncing core tables");
        loop {
            let node_height = get_node_height_blocking(session);
            if node_height <= session.head.height {
                break;
            }
            sync_to_height(session, node_height)?;
        }
        info!("Minimal sync completed");
        Ok(())
    }

    pub fn db_is_bootstrapped(session: &Session) -> bool {
        // Get last height of derived tables.
        let bootstrap_height = session.db.get_bootstrap_height().unwrap();
        bootstrap_height == session.head.height as i32
    }

    /// Fill derived tables to match sync height of core tables.
    pub fn expand_db(session: &mut Session) -> Result<(), &'static str> {
        info!("Bootstrapping step 2/2 - populating secondary tables");
        // Set db constraints if absent.
        // Constraints may already be set if bootstrap process got interrupted.
        if !session.db_constraints_set {
            session.load_db_constraints()?;
        }

        // Get last height of derived tables
        let bootstrap_height: i32 = session.db.get_bootstrap_height().unwrap();

        // Iterate from session.head.height to core_height
        // Run queries for each block height
        for h in bootstrap_height + 1..session.head.height as i32 + 1 {
            info!("Processing block {}/{}", h, session.head.height);
            // Collect statements
            let mut sql_statements: Vec<db::SQLStatement> = vec![];
            sql_statements.append(&mut db::unspent::prep_bootstrap(h));
            sql_statements.append(&mut db::balances::prep_bootstrap(h));

            // Execute statements in single transaction
            session.db.execute_in_transaction(sql_statements).unwrap();
        }
        info!("Bootstrapping completed");
        Ok(())
    }

    /// Sync db to given height
    ///
    /// No rollback support.
    fn sync_to_height(session: &mut Session, node_height: u32) -> Result<(), &'static str> {
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
        // Init parsing units
        let sql_statements = db::core::prep(block);

        // Execute statements in single transaction
        session.db.execute_in_transaction(sql_statements).unwrap();
    }
}

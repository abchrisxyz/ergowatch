mod db;
mod node;
mod settings;
mod types;
mod units;

use clap::Parser;
use log::debug;
use log::error;
use log::info;
use log::warn;
use std::{thread, time};

mod session;
use session::prepare_session;

// TODO: move this to config
const POLL_INTERVAL_SECONDS: u64 = 5;

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Cli {
    /// Path to config file
    #[clap(short, long)]
    config: Option<String>,

    /// Print help information
    #[clap(short, long)]
    help: bool,

    /// Allow migrations
    #[clap(short = 'm', long)]
    allow_migrations: bool,

    /// Use bootsrap mode
    #[clap(short = 'b', long)]
    bootstrap: bool,

    /// Path to constraints sql
    #[clap(short = 'k', long)]
    constraints_file: Option<String>,

    /// Print version information
    #[clap(short, long)]
    version: bool,

    /// Exit once synced (mostly for integration tests)
    #[clap(short, long)]
    sync_once: bool,
}

fn main() -> Result<(), &'static str> {
    let session = prepare_session()?;
    let node = &session.node;

    let mut head = session.get_db_sync_state()?;
    info!(
        "Database is currently at height {} with block {}",
        head.height, head.header_id
    );

    // TODO: avoid doing potentially more than once on rerun
    if head.height == 0 {
        session.include_genesis_boxes().unwrap();
    }

    loop {
        let node_height = match node.get_height() {
            Ok(h) => h,
            Err(e) => {
                error!("{}", e);
                info!("Retrying in {} seconds", POLL_INTERVAL_SECONDS);
                thread::sleep(time::Duration::from_secs(POLL_INTERVAL_SECONDS));
                continue;
            }
        };

        if node_height <= head.height {
            if session.sync_once {
                debug!("Done syncing, exiting now");
                return Ok(());
            }
            debug!("No new blocks - waiting {} seconds", POLL_INTERVAL_SECONDS);
            thread::sleep(time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            continue;
        }

        while head.height < node_height {
            let next_height = head.height + 1;
            // Fetch next block from node
            let block = node.get_main_chain_block_at(next_height).unwrap();

            if block.header.parent_id == head.header_id {
                info!(
                    "Including block {} for height {}",
                    block.header.id, block.header.height
                );

                // Collect statements
                let prepped_block = units::BlockData::new(&block);
                session.include_block(&prepped_block);

                // Move head to latest block
                head.height = next_height;
                head.header_id = block.header.id;
            } else {
                // New block is not a child of last processed block, need to rollback.
                warn!(
                    "Rolling back block {} at height {}",
                    head.header_id, head.height
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
                let block = node.get_block(&head.header_id).unwrap();

                // Collect rollback statements, in reverse order
                let prepped_block = units::BlockData::new(&block);
                session.rollback_block(&prepped_block);

                // Move head to previous block
                head.height = block.header.height - 1;
                head.header_id = block.header.parent_id;
            }
        }

        if session.bootstrapping {
            info!("Staring bootstrap process");
            session.load_db_constraints()?;
            session.run_bootstrapping_queries()?;
            info!("Done bootstrapping, exiting now");
            return Ok(());
        } else {
            info!("Database is synced - waiting for next block")
        }
    }
}

mod db;
mod node;
mod settings;
mod types;
mod units;

use clap::Parser;
use log::debug;
use log::error;
use log::info;
use std::{thread, time};

use settings::Settings;

// const DB_VERSION: i32 = 1;
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

    /// Print version information
    #[clap(short, long)]
    version: bool,

    /// Exit once synced
    #[clap(short, long)]
    sync_only: bool,
}

// TODO: add return codes (for test bench)
fn main() -> Result<(), &'static str> {
    env_logger::init();
    info!("Starting Ergo Watcher");

    let cli = Cli::parse();
    if cli.sync_only {
        info!("Found option `--sync-only`, watcher will exit once synced with node")
    }

    let cfg = match Settings::new(cli.config) {
        Ok(cfg) => cfg,
        Err(err) => {
            error!("{}", err);
            return Err("Failed loading config");
        }
    };
    let node = node::Node::new(cfg.node.url);

    let db = db::DB::new(
        &cfg.database.host,
        cfg.database.port,
        &cfg.database.name,
        &cfg.database.user,
        &cfg.database.pw,
    );

    // ToDo: check db version

    let mut head = db.get_head().unwrap();
    info!("Database is currently at block {}", head.height);

    // Parsing units
    let core = units::core::CoreUnit {};

    loop {
        let node_height = node.get_height().unwrap();

        if node_height <= head.height {
            if cli.sync_only {
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
            let header_id = node.get_block_at(next_height).unwrap();
            let block = node.get_block(header_id).unwrap();

            if block.header.parent_id == head.header_id {
                info!(
                    "Including block {} for height {}",
                    block.header.id, block.header.height
                );

                // Collect statements
                let prepped_block = units::BlockData::new(&block);
                let sql_statements = core.prep(&prepped_block);

                // Execute statements in single transaction
                db.execute_in_transaction(sql_statements).unwrap();

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

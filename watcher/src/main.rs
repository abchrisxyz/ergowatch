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

use settings::Settings;

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

    /// Print version information
    #[clap(short, long)]
    version: bool,

    /// Exit once synced
    #[clap(short, long)]
    sync_only: bool,
}

fn main() -> Result<(), &'static str> {
    let env = env_logger::Env::default().filter_or("EW_LOG", "info");
    env_logger::init_from_env(env);
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

    // Check db version
    match db.check_migrations(cli.allow_migrations) {
        Ok(_) => (),
        Err(e) => {
            error!("{}", e);
            return Err("Database not ready");
        }
    };

    // Get db state
    let mut head = match db.get_head() {
        Ok(h) => h,
        Err(e) => {
            error!("{}", e);
            return Err("Failed to retrieve db state");
        }
    };
    info!(
        "Database is currently at height {} with block {}",
        head.height, head.header_id
    );

    // Init parsing units (just one for now)
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
            let block = node.get_main_chain_block_at(next_height).unwrap();

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
                // New block is not a child of last processed block, need to rollback.
                warn!(
                    "Rolling back block {} at height {}",
                    head.header_id, head.height
                );

                // Retrieve processed block from node
                let block = node.get_block(&head.header_id).unwrap();

                // Collect rollback statements, in reverse order
                let prepped_block = units::BlockData::new(&block);
                let sql_statements = core.prep_rollback(&prepped_block);

                // Execute statements in single transaction
                db.execute_in_transaction(sql_statements).unwrap();

                // Move head to previous block
                head.height = block.header.height - 1;
                head.header_id = block.header.parent_id;
            }
        }
    }
}

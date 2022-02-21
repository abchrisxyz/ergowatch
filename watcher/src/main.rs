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
use std::fs;
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

/// Session parameters
struct Session {
    db: db::DB,
    db_constraints_set: bool,
    node: node::Node,
    bootstrapping: bool,
    sync_once: bool,
    constraints_path: String,
}

/// Preflight tasks
///
/// Configures logger, checks for forbidden options
/// and returns resulting session options.
fn prepare_session() -> Result<Session, &'static str> {
    let env = env_logger::Env::default().filter_or("EW_LOG", "info");
    env_logger::init_from_env(env);
    info!("Starting Ergo Watcher");

    // Parse command line args
    let cli = Cli::parse();
    if cli.sync_once && cli.bootstrap {
        return Err("--sync-once and --bootstrap options cannot be used together");
    } else if cli.sync_once {
        info!("Found option `--sync-once`, watcher will exit once synced with node")
    } else if cli.bootstrap {
        info!("Found option `--bootstrap`, watcher will start in bootstrap mode")
    }

    // Load config
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

    // Check db state
    let db_is_empty = match db.is_empty() {
        Ok(set) => set,
        Err(e) => {
            error!("{}", e);
            return Err("Database is not ready");
        }
    };
    // Check db constraints
    let db_constraints_set = match db.has_constraints() {
        Ok(set) => set,
        Err(e) => {
            error!("{}", e);
            return Err("Database is not ready");
        }
    };
    if !db_constraints_set {
        warn!("Database is unconstrained");
    }
    if db_is_empty {
        info!("Database is empty");
    }

    if cli.bootstrap && db_constraints_set {
        return Err("Bootstrap mode cannot be used anymore because database constraints have already been set.");
    }

    if !db_is_empty && !db_constraints_set && !cli.bootstrap {
        return Err(
            "Cannot run on a non-empty unconstrained database without the --bootstrap option.",
        );
    }

    let bootstrapping = if db_is_empty && !db_constraints_set && !cli.bootstrap {
        info!("Using bootstrap mode for empty unconstrained database");
        true
    } else if cli.bootstrap && !db_constraints_set {
        true
    } else {
        false
    };

    // Check db version
    match db.check_migrations(cli.allow_migrations) {
        Ok(_) => (),
        Err(e) => {
            error!("{}", e);
            return Err("Database not ready");
        }
    };

    // Ensure constraints file is accessible if needed
    let constraints_path = match cli.constraints_file {
        Some(path) => String::from(&path),
        None => String::from("constraints.sql"),
    };
    if bootstrapping {
        if let Err(e) = fs::read_to_string(&constraints_path) {
            error!("{}", e);
            error!("Could not read constraints file '{}'", &constraints_path);
            return Err("Could not read constraints file");
        }
    }

    Ok(Session {
        db,
        db_constraints_set,
        node: node,
        bootstrapping,
        sync_once: cli.sync_once,
        constraints_path: constraints_path,
    })
}

fn main() -> Result<(), &'static str> {
    let session = prepare_session()?;
    let node = &session.node;

    let mut head = session.get_db_sync_state()?;
    info!(
        "Database is currently at height {} with block {}",
        head.height, head.header_id
    );

    if head.height == 0 {
        session.include_genesis_boxes().unwrap();
    }

    loop {
        let node_height = node.get_height().unwrap();

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
        }
    }
}

impl Session {
    /// Get db sync state
    fn get_db_sync_state(&self) -> Result<types::Head, &'static str> {
        let head = match self.db.get_head() {
            Ok(h) => h,
            Err(e) => {
                error!("{}", e);
                return Err("Failed to retrieve db state");
            }
        };
        Ok(head)
    }

    fn include_genesis_boxes(&self) -> Result<(), &'static str> {
        info!("Retrieving genesis boxes");
        let boxes = match self.node.get_genesis_blocks() {
            Ok(boxes) => boxes,
            Err(e) => {
                error!("{}", e);
                return Err("Failed to retrieve genesis boxes from node");
            }
        };
        let sql_statements = units::core::genesis::prep(boxes);
        self.db.execute_in_transaction(sql_statements).unwrap();
        Ok(())
    }

    fn include_block(&self, block: &units::BlockData) {
        // Init parsing units
        let ucore = units::core::CoreUnit {};
        let ubal = units::balances::BalancesUnit {};
        let sql_statements = ucore.prep(block);

        // Execute statements in single transaction
        self.db.execute_in_transaction(sql_statements).unwrap();
    }

    fn rollback_block(&self, block: &units::BlockData) {
        // Init parsing units
        let ucore = units::core::CoreUnit {};
        let ubal = units::balances::BalancesUnit {};
        let sql_statements = ucore.prep(block);

        // Collect rollback statements, in reverse order
        let sql_statements = ucore.prep_rollback(block);

        // Execute statements in single transaction
        self.db.execute_in_transaction(sql_statements).unwrap();
    }

    fn load_db_constraints(&self) -> Result<(), &'static str> {
        info!(
            "Loading database constraints - close any other db connections to avoid relation locks"
        );
        assert_eq!(self.bootstrapping, true);
        // Load db constraints from file
        let sql = match fs::read_to_string(&self.constraints_path) {
            Ok(sql) => sql,
            Err(e) => {
                error!("{}", e);
                error!(
                    "Could not read constraints file '{}'",
                    &self.constraints_path
                );
                return Err("Could not read constraints file after bootstrapping");
            }
        };
        match self.db.apply_constraints(sql) {
            Ok(()) => info!("Database constraints have been loaded"),
            Err(e) => {
                error!("{}", e);
                return Err("Failed to set database constraints.");
            }
        };
        Ok(())
    }

    fn run_bootstrapping_queries(&self) -> Result<(), &'static str> {
        info!("Running bootstrapping queries");
        let sql_statements = units::balances::prep_bootstrap();
        self.db.execute_in_transaction(sql_statements).unwrap();
        Ok(())
    }
}

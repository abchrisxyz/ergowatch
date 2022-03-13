/// Brings together settings, cli and manages node and db access.
use clap::Parser;
use log::error;
use log::info;
use std::fs;

use crate::db;
use crate::node;
use crate::settings::Settings;

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
    #[clap(long)]
    no_bootstrap: bool,

    /// Path to constraints sql
    #[clap(short = 'k', long)]
    constraints_file: Option<String>,

    /// Print version information
    #[clap(short, long)]
    version: bool,

    /// Exit once synced (mostly for integration tests)
    #[clap(short = 'x', long)]
    exit: bool,
}

/// Session parameters
pub struct Session {
    pub db: db::DB,
    pub db_constraints_set: bool,
    pub db_is_empty: bool,
    pub node: node::Node,
    pub allow_bootstrap: bool,
    pub exit_when_synced: bool,
    pub constraints_path: String,
    pub head: crate::types::Head,
}

// Common tasks for both normal and bootstrap mode
impl Session {
    /// Prepare a new session
    ///
    /// Configures logger, checks for forbidden options and returns resulting session options.
    pub fn new() -> Result<Session, &'static str> {
        let env = env_logger::Env::default().filter_or("EW_LOG", "info");
        env_logger::init_from_env(env);
        info!("Starting Ergo Watcher {}", clap::crate_version!());

        let cli = Cli::parse();
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
        // Retrieve DB state
        let db_is_empty = match db.is_empty() {
            Ok(empty) => empty,
            Err(e) => {
                error!("{}", e);
                return Err("Failed connecting to database");
            }
        };
        let db_constraints_set = db.has_constraints().unwrap();
        let db_core_head = db.get_head().unwrap();
        let db_bootstrap_height = db.get_bootstrap_height().unwrap();

        // Check cli args and db state
        if db_is_empty && db_constraints_set {
            return Err(
                "Database should be initialized without constraints or indexes (schema.sql only)",
            );
        }
        if cli.no_bootstrap && db_bootstrap_height < db_core_head.height as i32 {
            return Err("Cannot use --no-boostrap after unfinished bootstrapping");
        }
        if cli.no_bootstrap {
            info!("Found --no-bootstrap flag, boostrapping process will be skipped");
        }
        if cli.allow_migrations {
            info!("Found option `--allow-migrations`, watcher will apply migrations if needed")
        }
        if cli.exit {
            info!("Found option `--exit`, watcher will exit once synced with node")
        }

        // Ensure constraints file is accessible.
        // Do it now cause it's a pain to find out halfway through bootstrap process.
        let constraints_path = match cli.constraints_file {
            Some(path) => String::from(&path),
            None => String::from("constraints.sql"),
        };
        if !db_constraints_set {
            if let Err(e) = fs::read_to_string(&constraints_path) {
                error!("{}", e);
                error!("Could not read constraints file '{}'", &constraints_path);
                error!("Specify a valid path using the -k option");
                return Err("Could access constraints file");
            }
        }

        // Check db version and migrations if allowed
        db.check_migrations(cli.allow_migrations).unwrap();

        Ok(Session {
            db,
            db_constraints_set,
            db_is_empty,
            node: node,
            allow_bootstrap: !cli.no_bootstrap,
            exit_when_synced: cli.exit,
            constraints_path: constraints_path,
            head: db_core_head,
        })
    }

    /// Create database constraints and indexes
    pub fn load_db_constraints(&mut self) -> Result<(), &'static str> {
        assert_eq!(self.db_constraints_set, false);
        info!(
            "Loading database constraints - close any other db connections to avoid relation locks"
        );
        // Load db constraints from file
        let sql = match fs::read_to_string(&self.constraints_path) {
            Ok(sql) => sql,
            Err(e) => {
                error!("{}", e);
                error!(
                    "Could not read constraints file '{}'",
                    self.constraints_path
                );
                return Err("Could not read constraints file");
            }
        };
        match self.db.apply_constraints(sql) {
            Ok(()) => {
                info!("Database constraints have been loaded");
                self.db_constraints_set = true;
            }
            Err(e) => {
                error!("{}", e);
                return Err("Failed to set database constraints.");
            }
        };
        Ok(())
    }

    // /// Add genesis boxes to database
    // pub fn include_genesis_boxes(&self) -> Result<(), &'static str> {
    //     info!("Retrieving genesis boxes");
    //     let boxes = match self.node.get_genesis_blocks() {
    //         Ok(boxes) => boxes,
    //         Err(e) => {
    //             error!("{}", e);
    //             return Err("Failed to retrieve genesis boxes from node");
    //         }
    //     };
    //     let sql_statements = units::core::genesis::prep(boxes);
    //     self.db.execute_in_transaction(sql_statements).unwrap();
    //     Ok(())
    // }

    // pub fn is_bootstrapping(&self) -> bool {
    //     match self.bootstrap_status {
    //         BootstrapStatus::Pending | BootstrapStatus::Started => true,
    //         _ => false,
    //     }
    // }
}

// // Normal model syncing
// impl Session {
//     /// Normal mode sync
//     ///
//     /// Syncs DB to given node_height and returns.
//     pub fn sync_to(&mut self, node_height: u32) -> Result<(), &'static str> {
//         while self.head.height < node_height {
//             let next_height = self.head.height + 1;
//             // Fetch next block from node
//             let block = self.node.get_main_chain_block_at(next_height).unwrap();

//             if block.header.parent_id == self.head.header_id {
//                 info!(
//                     "Including block {} for height {}",
//                     block.header.id, block.header.height
//                 );

//                 let prepped_block = units::BlockData::new(&block);
//                 self.include_block(&prepped_block);

//                 // Move head to latest block
//                 self.head.height = next_height;
//                 self.head.header_id = block.header.id;
//             } else {
//                 // New block is not a child of last processed block, need to rollback.
//                 warn!(
//                     "Rolling back block {} at height {}",
//                     self.head.header_id, self.head.height
//                 );

//                 // Rollbacks may rely on database constraints to propagate.
//                 // So prevent any rollbacks if constraints haven't been set.
//                 if !self.db_constraints_set {
//                     warn!("Preventing a rollback on an unconstrained database.");
//                     warn!("Rollbacks may rely on database constraints to propagate.");
//                     warn!("Please set the database contraints defined in `constraints.sql`.");
//                     return Err("Preventing a rollback on an unconstrained database.");
//                 }

//                 // Retrieve processed block from node
//                 let block = self.node.get_block(&self.head.header_id).unwrap();

//                 // Collect rollback statements, in reverse order
//                 let prepped_block = units::BlockData::new(&block);
//                 self.rollback_block(&prepped_block);

//                 // Move head to previous block
//                 self.head.height = block.header.height - 1;
//                 self.head.header_id = block.header.parent_id;
//             }
//         }
//         Ok(())
//     }

//     /// Process block data into database
//     fn include_block(&self, block: &units::BlockData) {
//         // Init parsing units
//         let ucore = units::core::CoreUnit {};
//         let mut sql_statements = ucore.prep(block);

//         // Skip bootstrappable units if bootstrapping
//         if self.bootstrap_status != BootstrapStatus::Started {
//             sql_statements.append(&mut units::unspent::prep(block));
//             sql_statements.append(&mut units::balances::prep(block));
//         }

//         // Execute statements in single transaction
//         self.db.execute_in_transaction(sql_statements).unwrap();
//     }

//     /// Discard block data from database
//     fn rollback_block(&self, block: &units::BlockData) {
//         // Init parsing units
//         let ucore = units::core::CoreUnit {};

//         // Collect rollback statements, in reverse order
//         let mut sql_statements: Vec<db::SQLStatement> = vec![];
//         sql_statements.append(&mut units::balances::prep_rollback(block));
//         sql_statements.append(&mut units::unspent::prep_rollback(block));
//         sql_statements.append(&mut ucore.prep_rollback(block));

//         // Execute statements in single transaction
//         self.db.execute_in_transaction(sql_statements).unwrap();
//     }
// }

// // Bootstrapping functions
// impl Session {
//     pub fn load_db_constraints(&self) -> Result<(), &'static str> {
//         info!(
//             "Loading database constraints - close any other db connections to avoid relation locks"
//         );
//         assert_eq!(self.is_bootstrapping(), true);
//         // Load db constraints from file
//         let sql = match fs::read_to_string(&self.constraints_path) {
//             Ok(sql) => sql,
//             Err(e) => {
//                 error!("{}", e);
//                 error!(
//                     "Could not read constraints file '{}'",
//                     &self.constraints_path
//                 );
//                 return Err("Could not read constraints file after bootstrapping");
//             }
//         };
//         match self.db.apply_constraints(sql) {
//             Ok(()) => info!("Database constraints have been loaded"),
//             Err(e) => {
//                 error!("{}", e);
//                 return Err("Failed to set database constraints.");
//             }
//         };
//         Ok(())
//     }

//     /// Fill derived tables to match sync height of core tables.
//     pub fn bootstrap_derived_tables(&mut self) -> Result<(), &'static str> {
//         assert_eq!(self.is_bootstrapping(), true);
//         // Set db constraints if absent
//         if !self.db_constraints_set {
//             self.load_db_constraints()?;
//         }
//         // Get last height of derived tables
//         let bootstrap_height: i32 = self.db.get_bootstrap_height().unwrap() as i32;

//         // Iterate from session.head.height to core_height
//         // Run queries for each block height
//         for h in bootstrap_height + 1..self.head.height as i32 + 1 {
//             // Collect statements
//             let mut sql_statements: Vec<db::SQLStatement> = vec![];
//             sql_statements.append(&mut units::unspent::prep_bootstrap(h));
//             sql_statements.append(&mut units::balances::prep_bootstrap(h));

//             // Execute statements in single transaction
//             self.db.execute_in_transaction(sql_statements).unwrap();
//         }
//         self.bootstrap_status = BootstrapStatus::Completed;

//         info!("Bootstrapping completed - proceeding in normal mode");
//         Ok(())
//     }
// }

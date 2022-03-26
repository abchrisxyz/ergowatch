/// Brings together settings, cli and manages node and db access.
use clap::Parser;
use log::error;
use log::info;

use crate::db;
use crate::node;
use crate::settings::Settings;

#[derive(Parser, Debug)]
#[clap(version, about, long_about = None)]
struct Cli {
    /// Path to config file
    #[clap(short, long, value_names = &["PATH"])]
    config: Option<String>,

    /// Print help information
    #[clap(short, long)]
    help: bool,

    /// Allow database migrations to be applied
    #[clap(short = 'm', long)]
    allow_migrations: bool,

    /// Skip bootstrap process
    #[clap(long)]
    no_bootstrap: bool,

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
    pub db_is_empty: bool,
    pub node: node::Node,
    pub allow_bootstrap: bool,
    pub exit_when_synced: bool,
    pub head: crate::types::Head,
    pub allow_rollbacks: bool,
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
        let db_constraints_status = db.constraints_status().unwrap();
        let db_core_head = db.get_head().unwrap();
        let unfinished_bootstrap = match db.get_bootstrap_height().unwrap() {
            Some(h) => h < db_core_head.height as i32,
            None => false,
        };

        // Check cli args and db state
        if db_is_empty && db_constraints_status.tier_1 {
            return Err(
                "Database should be initialized without constraints or indexes. Pass --no-bootstrap flag to override.",
            );
        }
        if cli.no_bootstrap && unfinished_bootstrap {
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

        // Check db version and migrations if allowed
        db.check_migrations(cli.allow_migrations).unwrap();

        Ok(Session {
            db,
            db_is_empty,
            node: node,
            allow_bootstrap: !cli.no_bootstrap,
            exit_when_synced: cli.exit,
            head: db_core_head,
            // If not
            allow_rollbacks: cli.no_bootstrap || db_constraints_status.all_set,
        })
    }
}

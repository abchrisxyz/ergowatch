/// Brings together settings, cli and manages node and db access.
use clap::Parser;
use log::error;
use log::info;

use crate::coingecko::CoingeckoService;
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

    /// Print version information
    #[clap(short, long)]
    version: bool,

    /// Exit once synced (mostly for integration tests)
    #[clap(short = 'x', long)]
    exit: bool,
}

/// Session parameters
pub struct Session {
    pub coingecko: CoingeckoService,
    pub db: db::DB,
    pub node: node::Node,
    pub poll_interval: u64,
    pub exit_when_synced: bool,
    pub head: crate::types::Head,
    pub allow_rollbacks: bool,
    pub repair_interval: u32,
    pub repair_offset: u32,
}

impl Session {
    /// Prepare a new session
    ///
    /// Configures logger, checks options and returns resulting session.
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
        let coingecko = CoingeckoService::new(cfg.coingecko.url, cfg.coingecko.interval);
        let node = node::Node::new(cfg.node.url);
        let db = db::DB::new(
            &cfg.database.host,
            cfg.database.port,
            &cfg.database.name,
            &cfg.database.user,
            &cfg.database.pw,
            cfg.database.bootstrapping_work_mem_kb,
        );

        // Retrieve DB state
        let db_is_empty = match db.is_empty() {
            Ok(empty) => empty,
            Err(e) => {
                error!("{}", e);
                return Err("Failed connecting to database");
            }
        };
        // let db_constraints_status = db.constraints_status().unwrap();
        let db_has_constraints = db.has_constraints().unwrap();
        let db_core_head = db.get_head().unwrap();

        // Check cli args and db state
        if db_is_empty && db_has_constraints {
            return Err(
                "Database should be initialized without constraints or indexes. Reinitialize the database using `schema.sql` only.",
            );
        }
        if cli.allow_migrations {
            info!("Found option `--allow-migrations`, watcher will apply migrations if needed")
        }
        if cli.exit {
            info!("Found option `--exit`, watcher will exit once synced with node")
        }

        // Cleanup remnants of possible interrupted repair session
        db.cleanup_interrupted_repair();

        // Check db version and migrations if allowed
        db.check_migrations(cli.allow_migrations).unwrap();

        Ok(Session {
            coingecko,
            db,
            node: node,
            poll_interval: cfg.node.poll_interval,
            exit_when_synced: cli.exit,
            head: db_core_head,
            allow_rollbacks: db_has_constraints,
            repair_interval: cfg.repairs.interval,
            repair_offset: cfg.repairs.offset,
        })
    }
}

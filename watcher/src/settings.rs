use config::{Config, ConfigError, File};
use log::info;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize)]
pub struct Database {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub user: String,
    pub pw: String,
    pub bootstrapping_work_mem_kb: u32,
}

#[derive(Debug, Deserialize)]
pub struct Node {
    pub url: String,
    // Time between two node polls, in seconds
    pub poll_interval: u64,
}

#[derive(Debug, Deserialize)]
pub struct Deposits {
    /// Minimum number of blocks to be processed in a batch
    pub interval: u32,
    // Number of blocks from latest one to exclude from processing batch
    pub buffer: u32,
}

#[derive(Debug, Deserialize)]
pub struct Coingecko {
    /// ERG/USD price api
    pub url: String,
    /// Time between consecutive requests, in seconds
    pub interval: f32,
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    pub database: Database,
    pub node: Node,
    pub deposits: Deposits,
    pub coingecko: Coingecko,
}

impl Settings {
    pub fn new(path: Option<String>) -> Result<Self, ConfigError> {
        let mut s = Config::new();
        let cfg_path = match path {
            Some(p) => p.clone(),
            None => String::from("local.toml"),
        };
        info!("Reading config from {}", cfg_path);
        s.merge(File::with_name(&cfg_path))?;

        match env::var("EW_DB_HOST") {
            Ok(value) => {
                info!("Found EW_DB_HOST environment variable");
                s.set("db.host", value).unwrap();
            }
            Err(_) => (),
        };

        match env::var("EW_DB_PORT") {
            Ok(value) => {
                info!("Found EW_DB_PORT environment variable");
                s.set("db.port", value).unwrap();
            }
            Err(_) => (),
        };

        match env::var("EW_DB_NAME") {
            Ok(value) => {
                info!("Found EW_DB_NAME environment variable");
                s.set("db.name", value).unwrap();
            }
            Err(_) => (),
        };

        match env::var("EW_DB_USER") {
            Ok(value) => {
                info!("Found EW_DB_USER environment variable");
                s.set("db.user", value).unwrap();
            }
            Err(_) => (),
        };

        match env::var("EW_DB_PASS") {
            Ok(value) => {
                info!("Found EW_DB_PASS environment variable");
                s.set("db.pw", value).unwrap();
            }
            Err(_) => (),
        };

        match env::var("EW_NODE_URL") {
            Ok(value) => {
                info!("Found EW_NODE_URL environment variable");
                s.set("node.url", value).unwrap();
            }
            Err(_) => (),
        };

        s.try_into()
    }
}

use config::{Config, ConfigError, File};
use log::info;
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Database {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub user: String,
    pub pw: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Node {
    pub url: String,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct Settings {
    pub database: Database,
    pub node: Node,
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

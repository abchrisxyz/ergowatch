use config::{Config, ConfigError, Environment, File};
use log::info;
use serde::Deserialize;

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
    pub debug: bool,
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
        // Add in settings from the environment (with a prefix of EW)
        s.merge(Environment::with_prefix("ew"))?;
        s.try_into()
    }
}

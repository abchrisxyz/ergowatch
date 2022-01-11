use config::{Config, ConfigError, Environment, File};
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
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        // ToDo take path from cli
        s.merge(File::with_name("local.toml")).unwrap();
        // Add in settings from the environment (with a prefix of EW)
        s.merge(Environment::with_prefix("ew")).unwrap();
        s.try_into()
    }
}
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// Postgresql connection URI postgresql://[userspec@][hostspec][/dbname][?paramspec]
    pub connection_uri: String,
}

impl PostgresConfig {
    pub fn new(uri: &str) -> Self {
        Self {
            connection_uri: uri.to_owned(),
        }
    }
}

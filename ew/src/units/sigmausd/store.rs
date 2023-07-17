use crate::config::PostgresConfig;
use crate::core::types::Head;

pub(super) struct Store {
    pgconf: PostgresConfig,
}

impl Store {
    pub(super) fn new(pgconf: PostgresConfig) -> Self {
        Self { pgconf }
    }

    pub(super) async fn head(&self) -> Head {
        tracing::warn!("Using dummy head");
        Head::initial()
    }
}

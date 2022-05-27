use crate::db::cexs;
use crate::db::metrics;

pub struct Cache {
    pub cexs: cexs::Cache,
    pub metrics: metrics::Cache,
}

impl Cache {
    /// Initialize a cache with default values, representing an empty database.
    pub fn new() -> Self {
        Self {
            cexs: cexs::Cache::new(),
            metrics: metrics::Cache::new(),
        }
    }
}

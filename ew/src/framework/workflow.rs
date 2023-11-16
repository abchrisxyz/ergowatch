use async_trait::async_trait;

use super::querying::QueryHandler;
use super::StampedData;
use crate::config::PostgresConfig;
use crate::core::types::Header;
use crate::core::types::Height;

#[async_trait]
pub trait Workflow {
    type U: Send + Sync; // upstream data
    type D: Send + Sync; // downstream data () for a sink

    /// Create and initialize and new workflow.
    async fn new(pgconf: &PostgresConfig) -> Self;

    // /// Handle genesis boxes.
    // async fn include_genesis_boxes(&mut self, data: &Self::U) -> Self::D;

    /// Process new block data.
    async fn include_block(&mut self, data: &StampedData<Self::U>) -> Self::D;

    /// Roll back a block and return previous head.
    async fn roll_back(&mut self, height: Height) -> Header;

    /// Get last processed header.
    fn header<'a>(&'a self) -> &'a Header;
}

/// Describes a workflow that can be turned into a source.
#[async_trait]
pub trait Sourceable {
    type S;

    /// Returns true if data for `header` has been included.
    async fn contains_header(&self, header: &Header) -> bool;

    /// Get data for given `head`.
    ///
    /// Used by lagging cursors to retrieve data.
    async fn get_at(&self, height: Height) -> StampedData<Self::S>;
}

/// Describes query-emitting workflows
#[async_trait]
pub trait Querying {
    type Q: Query;

    fn connect_to_query_handler(&mut self, query_handler: &QueryHandler<Self::Q>);
}

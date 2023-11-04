use async_trait::async_trait;

use super::StampedData;
use crate::config::PostgresConfig;
use crate::core::types::Head;
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
    async fn include_block(&mut self, data: &Self::U) -> Self::D;

    /// Roll back a block and return previous head.
    async fn roll_back(&mut self, height: Height) -> Head;

    /// Get last processed head.
    fn head<'a>(&'a self) -> &'a Head;
}

#[async_trait]
/// Describes a workflow that can be turned into a source.
pub trait Sourceable {
    type S;

    async fn contains_head(&self, head: &Head) -> bool;

    async fn get_at(&self, height: Height) -> StampedData<Self::S>;
}

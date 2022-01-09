pub mod core;

use crate::node::models::Block;
// use crate::types::Height;

/// Handles syncing of a specific dataset.
/// For lack of a better name.
pub trait Unit {
    /// Include block
    fn ingest(self: &mut Self, block: &Block) -> ();

    /// Rollback block
    fn rollback(self: &Self, block: &Block) -> ();
}

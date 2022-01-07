pub mod headers;

use crate::node::models::Block;
// use crate::types::Height;

/// Handles syncing of a specific dataset.
/// For lack of a better name.
pub trait Unit {
    /// Optional initialization
    // fn init() {}

    /// Include block
    fn ingest(self: &Self, block: &Block) -> ();

    /// Rollback block
    fn rollback(self: &Self, block: &Block) -> ();

    //// Get latest sync height
    // fn poll() -> Height;
}

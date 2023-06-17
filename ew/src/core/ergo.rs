//! Block preparation
//!
//! Handles rendering of encoded data such as output addresses,
//! token registers and header votes.
pub(super) mod ergo_tree;
pub(super) mod parsing;
pub(super) mod register;
pub(super) mod votes;

pub use parsing::Output;
pub use parsing::RenderedBlock;
pub use parsing::Transaction;

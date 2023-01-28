//! Block preparation
//!
//! Handles rendering of encoded data such as output addresses,
//! token registers and header votes.
mod ergo_tree;
mod parsing;
mod register;
mod votes;

pub use parsing::Output;
pub use parsing::RenderedBlock;
pub use parsing::Transaction;

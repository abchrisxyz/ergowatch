mod cursor;
mod event;
mod source;
pub mod store;
mod worker;
mod workflow;

/// Max number of events held by a channel between two workers.
pub const EVENT_CHANNEL_CAPACITY: usize = 8;

pub use cursor::Cursor;
pub use event::Event;
pub use event::Stamp;
pub use event::StampedData;
pub use source::Source;
pub use worker::SourceWorker;
pub use worker::Worker;
pub use workflow::Sourceable;
pub use workflow::Workflow;

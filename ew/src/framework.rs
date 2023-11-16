mod cursor;
mod event;
mod event_emission;
mod event_handling;
mod query_emission;
mod query_handling;
mod source;
pub mod store;
mod worker;

/// Max number of events held by a channel between two workers.
pub const EVENT_CHANNEL_CAPACITY: usize = 8;

pub use cursor::Cursor;
pub use event::Event;
pub use event::Stamp;
pub use event::StampedData;
pub use event_emission::EventEmission;
pub use event_handling::EventHandling;
pub use query_emission::QuerySender;
pub use query_emission::QueryWrapper;
pub use query_emission::Querying;
pub use query_handling::QueryHandling;
pub use source::Source;
pub use worker::LeafWorker;
pub use worker::QueryableSourceWorker;
pub use worker::SourceWorker;

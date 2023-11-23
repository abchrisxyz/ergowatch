use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;

use super::event::Event;
use crate::core::types::Header;

#[async_trait]
pub trait Source {
    type S: Send;

    fn header(&self) -> &Header;

    /// Returns true if `head` is part of source's processed main cahin.
    async fn contains_header(&self, header: &Header) -> bool;

    async fn subscribe(&mut self, header: Header, cursor_name: &str) -> Receiver<Event<Self::S>>;
}

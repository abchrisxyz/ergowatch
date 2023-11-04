use async_trait::async_trait;
use tokio::sync::mpsc::Receiver;

use super::event::Event;
use crate::core::types::Head;

#[async_trait]
pub trait Source {
    type S: Send;

    fn head(&self) -> &Head;

    /// Returns true if `head` is part of source's processed main cahin.
    async fn contains_head(&self, head: &Head) -> bool;

    async fn subscribe(&mut self, head: Head, cursor_name: &str) -> Receiver<Event<Self::S>>;
}

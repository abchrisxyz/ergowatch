use async_trait::async_trait;

use tokio::sync::mpsc::Receiver;

use crate::core::tracking::TrackingMessage;
use crate::core::types::CoreData;
use crate::core::types::Height;
use crate::core::types::Output;

pub mod sigmausd;

/// A parser extracts domain specific data from a block.
pub trait Parser {
    type B;
    fn parse_genesis_boxes(&self, outputs: &Vec<Output>) -> Self::B;
    fn parse(&self, data: &CoreData) -> Self::B;
}

/// A store handles persistence of domain specific data
#[async_trait]
pub trait Store {
    type B;
    async fn process(&self, batch: Self::B);
    async fn roll_back(&self, height: Height);
}

/// Workers listen for incoming events and arrange parsing and storage.
pub struct Worker<P, S>
where
    P: Parser,
    S: Store<B = P::B>,
{
    id: String,
    rx: Receiver<TrackingMessage>,
    parser: P,
    store: S,
}

impl<P, S> Worker<P, S>
where
    P: Parser,
    S: Store<B = P::B>,
{
    pub async fn start(&mut self) {
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("[{}] got a ctrl-c message", self.id);
                    break;
                },
                msg = self.rx.recv() => {
                    match msg.expect("message is some") {
                        TrackingMessage::Genesis(boxes) => &mut self.include_genesis_boxes(&boxes).await,
                        TrackingMessage::Include(data) => &mut self.include(&data).await,
                        TrackingMessage::Rollback(height) => &self.store.roll_back(height).await,
                    };
                },
            }
        }
    }

    async fn include_genesis_boxes(&self, outputs: &Vec<Output>) {
        let batch = self.parser.parse_genesis_boxes(outputs);
        self.store.process(batch).await;
    }

    async fn include(&self, data: &CoreData) {
        let batch = self.parser.parse(data);
        self.store.process(batch).await;
    }
}

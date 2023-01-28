use crate::render::RenderedBlock;
use std::sync::Arc;

#[derive(Debug)]
pub enum TrackingMessage {
    Include(Arc<RenderedBlock>),
    Rollback(Arc<RenderedBlock>),
}

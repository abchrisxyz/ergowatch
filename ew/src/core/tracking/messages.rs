use crate::core::types::CoreData;
use crate::core::types::Height;
use crate::core::types::Output;
use std::sync::Arc;

#[derive(Debug)]
pub enum TrackingMessage {
    Include(Arc<CoreData>),
    Rollback(Height),
    Genesis(Vec<Output>),
}

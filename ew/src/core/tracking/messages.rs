use std::sync::Arc;

use crate::core::types::BoxData;
use crate::core::types::CoreData;
use crate::core::types::Height;

#[derive(Debug)]
pub enum TrackingMessage {
    Include(Arc<CoreData>),
    Rollback(Height),
    Genesis(Vec<BoxData>),
}

use std::sync::Arc;

use crate::core::types::CoreData;
use crate::core::types::HeaderID;
use crate::core::types::Height;
use crate::core::types::Timestamp;

pub enum Event<D> {
    Include(Arc<StampedData<D>>),
    /// Roll back last block. Contains the height to be rolled back
    Rollback(Height),
}

impl<D> Event<D> {
    pub fn shallow_copy(&self) -> Self {
        match self {
            Self::Include(arc) => Self::Include(arc.clone()),
            Self::Rollback(h) => Self::Rollback(*h),
        }
    }
}

pub struct Stamp {
    pub height: Height,
    pub header_id: HeaderID,
    pub parent_id: HeaderID,
}

pub struct StampedData<D> {
    pub height: Height,
    pub timestamp: Timestamp,
    pub header_id: HeaderID,
    pub parent_id: HeaderID,
    pub data: D,
}

impl<D> StampedData<D> {
    /// Creates a new instance wrapping given `data`.
    pub fn wrap<T>(&self, data: T) -> StampedData<T> {
        StampedData {
            height: self.height,
            timestamp: self.timestamp,
            header_id: self.header_id.clone(),
            parent_id: self.parent_id.clone(),
            data,
        }
    }
}

#[cfg(feature = "test-utilities")]
impl From<CoreData> for StampedData<CoreData> {
    fn from(value: CoreData) -> Self {
        Self {
            height: value.block.header.height,
            timestamp: value.block.header.timestamp,
            header_id: value.block.header.id.clone(),
            parent_id: value.block.header.parent_id.clone(),
            data: value,
        }
    }
}

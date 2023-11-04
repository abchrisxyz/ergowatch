use std::sync::Arc;

use crate::core::types::{HeaderID, Height};

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
    pub header_id: HeaderID,
    pub parent_id: HeaderID,
    pub data: D,
}

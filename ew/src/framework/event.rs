use std::sync::Arc;

use crate::core::types::Header;
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

pub enum HandledEvent<D> {
    /// Downstream data from processing a new block event
    Include(StampedData<D>),
    /// New (previous) header resulting from a rollback.
    Rollback(Header),
    /// Skipped event (e.g. prior to a worker's start height)
    Skipped,
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
    /// Create a new `StampedData` from given `header` and `data`
    pub fn new(header: Header, data: D) -> Self {
        Self {
            height: header.height,
            timestamp: header.timestamp,
            header_id: header.header_id,
            parent_id: header.parent_id,
            data,
        }
    }

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
impl<D: Clone> StampedData<D> {
    /// Creates a new stamped
    pub fn wrap_as_child(&self, data: D) -> Self {
        StampedData {
            height: self.height + 1,
            timestamp: self.timestamp + 120_000,
            header_id: crate::core::types::testutils::random_digest32(),
            parent_id: self.header_id.clone(),
            data,
        }
    }

    /// Returns copy with timestamp set to given `timestamp`.
    pub fn timestamp(&self, timestamp: Timestamp) -> Self {
        StampedData {
            height: self.height,
            timestamp: timestamp,
            header_id: self.header_id.clone(),
            parent_id: self.parent_id.clone(),
            data: self.data.clone(),
        }
    }

    /// Extracts a Header from StampedData
    pub fn get_header(&self) -> Header {
        Header {
            height: self.height,
            timestamp: self.timestamp,
            header_id: self.header_id.clone(),
            parent_id: self.parent_id.clone(),
        }
    }
}

#[cfg(feature = "test-utilities")]
impl From<crate::core::types::CoreData> for StampedData<crate::core::types::CoreData> {
    fn from(value: crate::core::types::CoreData) -> Self {
        Self {
            height: value.block.header.height,
            timestamp: value.block.header.timestamp,
            header_id: value.block.header.id.clone(),
            parent_id: value.block.header.parent_id.clone(),
            data: value,
        }
    }
}

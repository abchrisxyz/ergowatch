use crate::core::types::Height;
use crate::core::types::Timestamp;

pub(super) struct Batch {
    pub(super) hourly: Vec<Action>,
    pub(super) daily: Vec<Action>,
    pub(super) weekly: Vec<Action>,
}

#[derive(Debug, PartialEq)]
pub(super) enum Action {
    INSERT(TimestampRecord),
    UPDATE(TimestampRecord),
    DELETE(Height),
}

impl Action {
    /// Return TimestampRecord held by an INSERT variant.
    ///
    /// Returns None for other variants.
    pub(super) fn get_inserted(&self) -> Option<TimestampRecord> {
        match &self {
            Self::INSERT(tr) => Some(tr.clone()),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub(super) struct TimestampRecord {
    pub(super) height: Height,
    pub(super) timestamp: Timestamp,
}

impl TimestampRecord {
    pub fn new(height: Height, timestamp: Timestamp) -> Self {
        Self { height, timestamp }
    }

    /// Dummy TimestampRecord representing state prior to genesis
    pub fn initial() -> Self {
        Self {
            height: -1,
            timestamp: 0,
        }
    }
}

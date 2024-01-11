use crate::core::types::Height;
use crate::core::types::Timestamp;

pub type MilliSeconds = Timestamp;

pub(super) struct Batch {
    pub block_record: BlockRecord,
    pub provisional_block_record: Option<ProvisionalBlockRecord>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HourlyRecord {
    pub timestamp: Timestamp,
    pub usd: f32,
}

impl HourlyRecord {
    pub fn new(timestamp: Timestamp, usd: f32) -> Self {
        Self { timestamp, usd }
    }

    pub fn genesis() -> Self {
        Self {
            timestamp: crate::constants::GENESIS_TIMESTAMP,
            // First available datapoint from CoinGecko - timestamp = 1561979001925
            usd: 5.581469768257971,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct BlockRecord {
    pub height: Height,
    pub usd: f32,
}
impl BlockRecord {
    pub fn new(height: Height, usd: f32) -> Self {
        Self { height, usd }
    }
}

/// Holds the height and timestamp of a provisional BlockRecord (one that hasn't been interpolated yet)
#[derive(Debug, PartialEq, Clone)]
pub struct ProvisionalBlockRecord {
    pub timestamp: Timestamp,
    pub height: Height,
}

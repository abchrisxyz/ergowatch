use rust_decimal::Decimal;

use crate::core::types::AddressID;
use crate::core::types::BoxID;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;

/// Data extracted from a block and ready to be stored.
pub struct Batch {
    pub head: Head,
    pub bank_transactions: Vec<BankTransaction>,
    pub oracle_posting: Option<OraclePosting>,
    pub history_record: Option<HistoryRecord>,
    pub ohlc_diff: Option<OHLCDiff>,
    pub service_diffs: Vec<ServiceStats>,
}

pub struct BankTransaction {
    pub index: i32,
    pub height: Height,
    pub reserves_diff: NanoERG,
    pub sc_diff: i64,
    pub rc_diff: i64,
    pub box_id: BoxID,
    pub service_fee: NanoERG,
    pub service_address_id: AddressID,
}

pub struct OraclePosting {
    pub height: Height,
    pub datapoint: i64,
    pub box_id: BoxID,
}

pub struct HistoryRecord {
    pub height: Height,
    pub oracle: i64,
    pub circ_sc: i64,
    pub circ_rc: i64,
    pub reserves: NanoERG,
    pub sc_net: NanoERG,
    pub rc_net: NanoERG,
}

pub struct OHLC {
    pub t: time::Date,
    pub o: f32,
    pub h: f32,
    pub l: f32,
    pub c: f32,
}

pub struct OHLCDiff {
    pub daily: OHLC,
    pub weekly: OHLC,
    pub monthly: OHLC,
}

pub struct ServiceStats {
    pub address_id: AddressID,
    pub tx_count: i64,
    pub first_tx: Timestamp,
    pub last_tx: Timestamp,
    pub fees: Decimal,
    pub volume: Decimal,
}

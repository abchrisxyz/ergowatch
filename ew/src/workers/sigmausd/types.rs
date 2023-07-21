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
    pub events: Vec<Event>,
    pub history_record: Option<HistoryRecord>,
    pub ohlc_records: Vec<OHLCRecord>,
    pub service_diffs: Vec<ServiceStats>,
}

pub enum Event {
    /// New oracle price
    Oracle(OraclePosting),
    /// Bank creation
    // InitialBankTx(BankTransaction),
    /// New bank transaction
    BankTx(BankTransaction),
}

pub struct BankTransaction {
    pub index: i32,
    pub height: Height,
    pub reserves_diff: NanoERG,
    pub circ_sc_diff: i64,
    pub circ_rc_diff: i64,
    pub box_id: BoxID,
    pub service_fee: NanoERG,
    pub service_address_id: Option<AddressID>,
}

pub struct OraclePosting {
    pub height: Height,
    pub datapoint: i64,
    pub box_id: BoxID,
}

#[derive(Clone)]
pub struct HistoryRecord {
    pub height: Height,
    pub oracle: i64,
    pub circ_sc: i64,
    pub circ_rc: i64,
    pub reserves: NanoERG,
    pub sc_net: NanoERG,
    pub rc_net: NanoERG,
}

pub enum OHLCRecord {
    Daily(OHLC),
    Weekly(OHLC),
    Monthly(OHLC),
}

pub struct OHLC {
    pub t: time::Date,
    pub o: f32,
    pub h: f32,
    pub l: f32,
    pub c: f32,
}

pub struct OHLCGroup {
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

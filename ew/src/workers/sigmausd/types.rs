use rust_decimal::Decimal;

use crate::core::types::AddressID;
use crate::core::types::BoxID;
use crate::core::types::Head;
use crate::core::types::HeaderID;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::core::types::Timestamp;

use super::constants::DEFAULT_RSV_PRICE;

/// Data extracted from a block and ready to be stored.
pub struct Batch {
    pub header: MiniHeader,
    pub events: Vec<Event>,
    pub history_record: Option<HistoryRecord>,
    pub daily_ohlc_records: Vec<DailyOHLC>,
    pub weekly_ohlc_records: Vec<WeeklyOHLC>,
    pub monthly_ohlc_records: Vec<MonthlyOHLC>,
    pub service_diffs: Vec<ServiceStats>,
}

pub struct MiniHeader {
    pub height: Height,
    pub timestamp: Timestamp,
    pub id: HeaderID,
}

impl MiniHeader {
    pub fn new(height: Height, timestamp: Timestamp, id: HeaderID) -> Self {
        Self {
            height,
            timestamp,
            id,
        }
    }

    pub fn head(&self) -> Head {
        Head::new(self.height, self.id.clone())
    }
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

impl HistoryRecord {
    pub fn rc_price(&self) -> NanoERG {
        let liabilities: NanoERG = self.oracle * self.circ_sc / 100;
        let equity = i64::max(0, self.reserves - liabilities);
        if self.circ_rc > 0 && equity > 0 {
            equity / self.circ_rc
        } else {
            DEFAULT_RSV_PRICE
        }
    }
}

// RC coin price in NanoERG
#[derive(Clone)]
pub struct OHLC {
    pub t: time::Date,
    pub o: NanoERG,
    pub h: NanoERG,
    pub l: NanoERG,
    pub c: NanoERG,
}

impl OHLC {
    /// Make a new OHLC record by combining a `prior` and `next`.
    ///
    /// Date must be equal. `prior` is assumed to have occured earlier
    /// and will provide the open. `next`is assumed to have occured later and
    /// will provice the close.
    pub fn merge(prior: &Self, next: &Self) -> Self {
        assert_eq!(prior.t, next.t);
        OHLC {
            t: prior.t,
            o: prior.o,
            h: std::cmp::max(prior.h, next.h),
            l: std::cmp::min(prior.l, next.l),
            c: next.c,
        }
    }
}

pub struct DailyOHLC(pub OHLC);
pub struct WeeklyOHLC(pub OHLC);
pub struct MonthlyOHLC(pub OHLC);

impl DailyOHLC {
    /// New daily OHLC from a collection of prices
    pub fn from_prices(t: Timestamp, rc_prices: &Vec<NanoERG>) -> Self {
        let date = time::OffsetDateTime::from_unix_timestamp(t / 1000)
            .unwrap()
            .date();
        let ohlc = OHLC {
            t: date,
            o: *rc_prices.first().unwrap(),
            h: *rc_prices.iter().max().unwrap(),
            l: *rc_prices.iter().min().unwrap(),
            c: *rc_prices.last().unwrap(),
        };
        Self(ohlc)
    }

    /// Returns a copy with date rounded to week's Monday.
    pub fn to_weekly(&self) -> WeeklyOHLC {
        let mut ohlc = self.0.clone();
        ohlc.t = self.0.t.prev_occurrence(time::Weekday::Monday);
        WeeklyOHLC(ohlc)
    }

    /// Returns a copy with date rounded to first of month.
    pub fn to_monthly(&self) -> MonthlyOHLC {
        let mut ohlc = self.0.clone();
        ohlc.t = ohlc.t.replace_day(1).unwrap();
        MonthlyOHLC(ohlc)
    }

    /// Generate daily OHLC series from `since` up to `self`.
    /// Merges `since` and `self` if both are on the same day.
    pub fn fill_since(self, since: &Self) -> Vec<Self> {
        // Since must be anterior
        assert!(since.0.t <= self.0.t);

        if since.0.t == self.0.t {
            return vec![Self(OHLC::merge(&since.0, &self.0))];
        }

        let mut records = vec![];
        let n_days_in_between = (self.0.t - since.0.t).whole_days() - 1;
        for _ in 0..n_days_in_between {
            records.push(since.next());
        }
        records.push(self);
        records
    }

    /// Returns copy of `self` with date incremented to next day.
    pub fn next(&self) -> Self {
        let ohlc = &self.0;
        Self(OHLC {
            t: ohlc.t.next_day().unwrap(),
            o: ohlc.o,
            h: ohlc.h,
            l: ohlc.l,
            c: ohlc.c,
        })
    }
}

impl WeeklyOHLC {
    /// Generate weekly OHLC series from `since` up to `self`.
    /// Merges `since` and `self` if both are on the same week.
    pub fn fill_since(self, since: &Self) -> Vec<Self> {
        // Since must be anterior
        assert!(since.0.t <= self.0.t);

        if since.0.t == self.0.t {
            return vec![Self(OHLC::merge(&since.0, &self.0))];
        }

        let mut records = vec![];
        let n_weeks_in_between = (self.0.t - since.0.t).whole_weeks() - 1;
        for _ in 0..n_weeks_in_between {
            records.push(since.next());
        }
        records.push(self);
        records
    }

    /// Returns copy of `self` with date incremented to next week.
    pub fn next(&self) -> Self {
        let ohlc = &self.0;
        Self(OHLC {
            t: ohlc.t + time::Duration::WEEK,
            o: ohlc.o,
            h: ohlc.h,
            l: ohlc.l,
            c: ohlc.c,
        })
    }
}

impl MonthlyOHLC {
    /// Generate monthly OHLC series from `since` up to `self`.
    /// Merges `since` and `self` if both are on the same month.
    pub fn fill_since(self, since: &Self) -> Vec<Self> {
        // Since must be anterior
        assert!(since.0.t <= self.0.t);

        if since.0.t == self.0.t {
            return vec![Self(OHLC::merge(&since.0, &self.0))];
        }

        let mut records = vec![];
        let dy = self.0.t.year() - since.0.t.year();
        let n_months_in_between = match dy {
            0 => self.0.t.month() as i32 - since.0.t.month() as i32,
            1 => self.0.t.month() as i32 + 12 - since.0.t.month() as i32,
            _ => self.0.t.month() as i32 + 12 - since.0.t.month() as i32 + dy * 12,
        } - 1;
        for _ in 0..n_months_in_between {
            records.push(since.next());
        }
        records.push(self);
        records
    }

    /// Returns copy of `self` with date incremented to next month.
    pub fn next(&self) -> Self {
        let ohlc = &self.0;
        let date = match ohlc.t.month() {
            time::Month::December => ohlc
                .t
                .replace_month(ohlc.t.month().next())
                .unwrap()
                .replace_year(ohlc.t.year() + 1),
            _ => ohlc.t.replace_month(ohlc.t.month().next()),
        }
        .unwrap();
        Self(OHLC {
            t: date,
            o: ohlc.o,
            h: ohlc.h,
            l: ohlc.l,
            c: ohlc.c,
        })
    }
}

pub struct OHLCGroup {
    pub daily: DailyOHLC,
    pub weekly: WeeklyOHLC,
    pub monthly: MonthlyOHLC,
}

pub struct ServiceStats {
    pub address_id: AddressID,
    pub tx_count: i64,
    pub first_tx: Timestamp,
    pub last_tx: Timestamp,
    pub fees: Decimal,
    pub volume: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_history_record_rc_price() {
        let hr = HistoryRecord {
            height: 10,
            oracle: 847457627,   // 1 ERG = 1.18 USD
            circ_sc: 44_837_610, // in cents
            circ_rc: 2_700_966_369,
            reserves: 1_494_997_124_719_372,
            sc_net: 0,
            rc_net: 0,
        };
        assert_eq!(hr.rc_price(), 412821);
    }

    #[test]
    fn test_history_record_rc_price_no_equity() {
        let hr = HistoryRecord {
            height: 10,
            oracle: 8474576270,  // 1 ERG = 10.18 USD
            circ_sc: 44_837_610, // in cents
            circ_rc: 2_700_966_369,
            reserves: 1_494_997_124_719_372,
            sc_net: 0,
            rc_net: 0,
        };
        assert_eq!(hr.rc_price(), DEFAULT_RSV_PRICE);
    }

    #[test]
    fn test_history_record_rc_price_no_circ_rc() {
        let hr = HistoryRecord {
            height: 10,
            oracle: 847457627,   // 1 ERG = 1.18 USD
            circ_sc: 44_837_610, // in cents
            circ_rc: 0,
            reserves: 1_494_997_124_719_372,
            sc_net: 0,
            rc_net: 0,
        };
        assert_eq!(hr.rc_price(), DEFAULT_RSV_PRICE);
    }

    #[test]
    fn test_daily_ohlc_from_prices() {
        let t = 1635347405599; // WED 2021-10-27;
        let rc_prices = vec![2, 1, 5, 3];
        let daily = DailyOHLC::from_prices(t, &rc_prices).0;
        assert_eq!(
            daily.t,
            time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap()
        );
        assert_eq!(daily.o, 2);
        assert_eq!(daily.h, 5);
        assert_eq!(daily.l, 1);
        assert_eq!(daily.c, 3);
    }

    #[test]
    fn test_daily_ohlc_from_single_price() {
        let t = 1635347405599; // WED 2021-10-27;
        let rc_prices = vec![5];
        let daily = DailyOHLC::from_prices(t, &rc_prices).0;
        assert_eq!(
            daily.t,
            time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap()
        );
        assert_eq!(daily.o, 5);
        assert_eq!(daily.h, 5);
        assert_eq!(daily.l, 5);
        assert_eq!(daily.c, 5);
    }

    #[test]
    fn test_daily_ohlc_to_weekly() {
        // WED 27 October 2021
        let daily = DailyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let weekly = daily.to_weekly();
        assert_eq!(
            weekly.0.t,
            time::Date::from_calendar_date(2021, time::Month::October, 25).unwrap()
        );
        assert_eq!(weekly.0.o, daily.0.o);
        assert_eq!(weekly.0.h, daily.0.h);
        assert_eq!(weekly.0.l, daily.0.l);
        assert_eq!(weekly.0.c, daily.0.c);
    }

    #[test]
    fn test_daily_ohlc_to_monthly() {
        // WED 27 October 2021
        let daily = DailyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let monthly = daily.to_monthly();
        assert_eq!(
            monthly.0.t,
            time::Date::from_calendar_date(2021, time::Month::October, 1).unwrap()
        );
        assert_eq!(monthly.0.o, daily.0.o);
        assert_eq!(monthly.0.h, daily.0.h);
        assert_eq!(monthly.0.l, daily.0.l);
        assert_eq!(monthly.0.c, daily.0.c);
    }

    #[test]
    fn test_daily_ohlc_fill_since_same_day() {
        let date = time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap();
        let since = DailyOHLC(OHLC {
            t: date,
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let daily = DailyOHLC(OHLC {
            t: date,
            o: 6,
            h: 9,
            l: 7,
            c: 8,
        });
        let dailys = daily.fill_since(&since);
        assert!(dailys.len() == 1);
        let ohlc = &dailys[0].0;
        assert_eq!(ohlc.t, date);
        assert_eq!(ohlc.o, 2);
        assert_eq!(ohlc.h, 9);
        assert_eq!(ohlc.l, 1);
        assert_eq!(ohlc.c, 8);
    }

    #[test]
    fn test_daily_ohlc_fill_since_previous_day() {
        let since = DailyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 26).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let daily = DailyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap(),
            o: 6,
            h: 9,
            l: 7,
            c: 8,
        });
        let dailys = daily.fill_since(&since);
        assert!(dailys.len() == 1);

        let ohlc = &dailys[0].0;
        assert_eq!(
            ohlc.t,
            time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap(),
        );
        assert_eq!(ohlc.o, 6);
        assert_eq!(ohlc.h, 9);
        assert_eq!(ohlc.l, 7);
        assert_eq!(ohlc.c, 8);
    }

    #[test]
    fn test_daily_ohlc_fill_since_anterior_day() {
        let since = DailyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 25).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let daily = DailyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap(),
            o: 6,
            h: 9,
            l: 7,
            c: 8,
        });
        let dailys = daily.fill_since(&since);
        assert!(dailys.len() == 2);

        let ohlc = &dailys[0].0;
        assert_eq!(
            ohlc.t,
            time::Date::from_calendar_date(2021, time::Month::October, 26).unwrap(),
        );
        assert_eq!(ohlc.o, since.0.o);
        assert_eq!(ohlc.h, since.0.h);
        assert_eq!(ohlc.l, since.0.l);
        assert_eq!(ohlc.c, since.0.c);

        let ohlc = &dailys[1].0;
        assert_eq!(
            ohlc.t,
            time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap(),
        );
        assert_eq!(ohlc.o, 6);
        assert_eq!(ohlc.h, 9);
        assert_eq!(ohlc.l, 7);
        assert_eq!(ohlc.c, 8);
    }

    #[test]
    fn test_weekly_ohlc_fill_since_same_week() {
        let date = time::Date::from_calendar_date(2021, time::Month::October, 25).unwrap();
        let since = WeeklyOHLC(OHLC {
            t: date,
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let weekly = WeeklyOHLC(OHLC {
            t: date,
            o: 6,
            h: 9,
            l: 7,
            c: 8,
        });
        let weeklys = weekly.fill_since(&since);
        assert!(weeklys.len() == 1);
        let ohlc = &weeklys[0].0;
        assert_eq!(ohlc.t, date);
        assert_eq!(ohlc.o, 2);
        assert_eq!(ohlc.h, 9);
        assert_eq!(ohlc.l, 1);
        assert_eq!(ohlc.c, 8);
    }

    #[test]
    fn test_weekly_ohlc_fill_since_previous_week() {
        let since = WeeklyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 18).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let weekly = WeeklyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 25).unwrap(),
            o: 6,
            h: 9,
            l: 7,
            c: 8,
        });
        let weeklys = weekly.fill_since(&since);
        assert!(weeklys.len() == 1);

        let ohlc = &weeklys[0].0;
        assert_eq!(
            ohlc.t,
            time::Date::from_calendar_date(2021, time::Month::October, 25).unwrap(),
        );
        assert_eq!(ohlc.o, 6);
        assert_eq!(ohlc.h, 9);
        assert_eq!(ohlc.l, 7);
        assert_eq!(ohlc.c, 8);
    }

    #[test]
    fn test_weekly_ohlc_fill_since_anterior_week() {
        let since = WeeklyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 11).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let weekly = WeeklyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 25).unwrap(),
            o: 6,
            h: 9,
            l: 7,
            c: 8,
        });
        let weeklys = weekly.fill_since(&since);
        assert!(weeklys.len() == 2);

        let ohlc = &weeklys[0].0;
        assert_eq!(
            ohlc.t,
            time::Date::from_calendar_date(2021, time::Month::October, 18).unwrap(),
        );
        assert_eq!(ohlc.o, since.0.o);
        assert_eq!(ohlc.h, since.0.h);
        assert_eq!(ohlc.l, since.0.l);
        assert_eq!(ohlc.c, since.0.c);

        let ohlc = &weeklys[1].0;
        assert_eq!(
            ohlc.t,
            time::Date::from_calendar_date(2021, time::Month::October, 25).unwrap(),
        );
        assert_eq!(ohlc.o, 6);
        assert_eq!(ohlc.h, 9);
        assert_eq!(ohlc.l, 7);
        assert_eq!(ohlc.c, 8);
    }

    #[test]
    fn test_monthly_ohlc_fill_since_same_month() {
        let date = time::Date::from_calendar_date(2021, time::Month::October, 1).unwrap();
        let since = MonthlyOHLC(OHLC {
            t: date,
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let monthly = MonthlyOHLC(OHLC {
            t: date,
            o: 6,
            h: 9,
            l: 7,
            c: 8,
        });
        let monthlys = monthly.fill_since(&since);
        assert!(monthlys.len() == 1);
        let ohlc = &monthlys[0].0;
        assert_eq!(ohlc.t, date);
        assert_eq!(ohlc.o, 2);
        assert_eq!(ohlc.h, 9);
        assert_eq!(ohlc.l, 1);
        assert_eq!(ohlc.c, 8);
    }

    #[test]
    fn test_monthly_ohlc_fill_since_previous_month() {
        let since = MonthlyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::September, 1).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let monthly = MonthlyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 1).unwrap(),
            o: 6,
            h: 9,
            l: 7,
            c: 8,
        });
        let monthlys = monthly.fill_since(&since);
        assert_eq!(monthlys.len(), 1);

        let ohlc = &monthlys[0].0;
        assert_eq!(
            ohlc.t,
            time::Date::from_calendar_date(2021, time::Month::October, 1).unwrap(),
        );
        assert_eq!(ohlc.o, 6);
        assert_eq!(ohlc.h, 9);
        assert_eq!(ohlc.l, 7);
        assert_eq!(ohlc.c, 8);
    }

    #[test]
    fn test_monthly_ohlc_fill_since_anterior_month() {
        let since = MonthlyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::December, 1).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let monthly = MonthlyOHLC(OHLC {
            t: time::Date::from_calendar_date(2022, time::Month::February, 1).unwrap(),
            o: 6,
            h: 9,
            l: 7,
            c: 8,
        });
        let monthlys = monthly.fill_since(&since);
        assert_eq!(monthlys.len(), 2);

        let ohlc = &monthlys[0].0;
        assert_eq!(
            ohlc.t,
            time::Date::from_calendar_date(2022, time::Month::January, 1).unwrap(),
        );
        assert_eq!(ohlc.o, since.0.o);
        assert_eq!(ohlc.h, since.0.h);
        assert_eq!(ohlc.l, since.0.l);
        assert_eq!(ohlc.c, since.0.c);

        let ohlc = &monthlys[1].0;
        assert_eq!(
            ohlc.t,
            time::Date::from_calendar_date(2022, time::Month::February, 1).unwrap(),
        );
        assert_eq!(ohlc.o, 6);
        assert_eq!(ohlc.h, 9);
        assert_eq!(ohlc.l, 7);
        assert_eq!(ohlc.c, 8);
    }
}

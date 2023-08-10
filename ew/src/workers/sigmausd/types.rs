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

#[derive(Clone)]
pub struct DailyOHLC(pub OHLC);

#[derive(Clone)]
pub struct WeeklyOHLC(pub OHLC);

#[derive(Clone)]
pub struct MonthlyOHLC(pub OHLC);

impl DailyOHLC {
    /// New daily OHLC from a collection of prices
    pub fn from_prices(t: Timestamp, rc_prices: &Vec<NanoERG>) -> Option<Self> {
        if rc_prices.is_empty() {
            return None;
        }
        let date = Self::date_from_timestamp(t);
        let ohlc = OHLC {
            t: date,
            o: *rc_prices.first().unwrap(),
            h: *rc_prices.iter().max().unwrap(),
            l: *rc_prices.iter().min().unwrap(),
            c: *rc_prices.last().unwrap(),
        };
        Some(Self(ohlc))
    }

    /// Helper to convert ergo timestamps to rounded dates.
    pub fn date_from_timestamp(t: Timestamp) -> time::Date {
        time::OffsetDateTime::from_unix_timestamp(t / 1000)
            .unwrap()
            .date()
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

    /// Returns any records between `self` and future `timestamp`.
    ///
    /// New records will have all prices set to `self`'s close.
    pub fn propagate_to(&self, to_date: &time::Date) -> Vec<Self> {
        assert!(to_date >= &self.0.t);
        let mut records = vec![];
        let n_days = (*to_date - self.0.t).whole_days();
        for _ in 0..n_days {
            records.push(self.next());
        }
        records
    }

    /// Returns new record with date incremented to next day and prices set to previous close.
    fn next(&self) -> Self {
        let ohlc = &self.0;
        Self(OHLC {
            t: ohlc.t.next_day().unwrap(),
            o: ohlc.c,
            h: ohlc.c,
            l: ohlc.c,
            c: ohlc.c,
        })
    }
}

impl WeeklyOHLC {
    /// Returns a copy of `daily` with date rounded to week's Monday.
    pub fn from_daily(daily: &DailyOHLC) -> WeeklyOHLC {
        let mut ohlc = daily.0.clone();
        ohlc.t = Self::round_date(&ohlc.t);
        WeeklyOHLC(ohlc)
    }

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

    /// Returns any records between `self` and future `timestamp`.
    ///
    /// New records will have all prices set to `self`'s close.
    pub fn propagate_to(&self, date: &time::Date) -> Vec<Self> {
        let to_date = Self::round_date(date);
        assert!(to_date >= self.0.t);
        let mut records = vec![];
        let n_weeks = (to_date - self.0.t).whole_weeks();
        for _ in 0..n_weeks {
            records.push(self.next());
        }
        records
    }

    /// Round date to monday of same week.
    fn round_date(date: &time::Date) -> time::Date {
        match date.weekday() {
            time::Weekday::Monday => date.clone(),
            _ => date.prev_occurrence(time::Weekday::Monday),
        }
    }

    /// Returns new record with date incremented to next week and prices set to previous close.
    fn next(&self) -> Self {
        let ohlc = &self.0;
        Self(OHLC {
            t: ohlc.t + time::Duration::WEEK,
            o: ohlc.c,
            h: ohlc.c,
            l: ohlc.c,
            c: ohlc.c,
        })
    }
}

impl MonthlyOHLC {
    /// Returns a copy of `daily` with date rounded to first of month.
    pub fn from_daily(daily: &DailyOHLC) -> MonthlyOHLC {
        let mut ohlc = daily.0.clone();
        ohlc.t = Self::round_date(&ohlc.t);
        MonthlyOHLC(ohlc)
    }

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

    /// Returns any records between `self` and future `timestamp`.
    ///
    /// New records will have all prices set to `self`'s close.
    pub fn propagate_to(&self, date: &time::Date) -> Vec<Self> {
        let to_date = Self::round_date(date);
        assert!(to_date >= self.0.t);
        let mut records = vec![];
        let dy = to_date.year() - self.0.t.year();
        let n_months = match dy {
            0 => to_date.month() as i32 - self.0.t.month() as i32,
            1 => to_date.month() as i32 + 12 - self.0.t.month() as i32,
            _ => to_date.month() as i32 + 12 - self.0.t.month() as i32 + dy * 12,
        };
        for _ in 0..n_months {
            records.push(self.next());
        }
        records
    }

    /// Round date to first of month.
    fn round_date(date: &time::Date) -> time::Date {
        date.replace_day(1).unwrap()
    }

    /// Returns new record with date incremented to next month and prices set to previous close.
    fn next(&self) -> Self {
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
            o: ohlc.c,
            h: ohlc.c,
            l: ohlc.c,
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
    // None when no service (direct interaction)
    pub address_id: Option<AddressID>,
    pub tx_count: i64,
    pub first_tx: Timestamp,
    pub last_tx: Timestamp,
    pub fees: Decimal,
    pub volume: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::date;

    impl OHLC {
        pub fn dummy() -> Self {
            Self {
                t: time::macros::date!(2023 - 01 - 13),
                o: 2,
                h: 4,
                l: 1,
                c: 3,
            }
        }
    }

    impl DailyOHLC {
        pub fn dummy() -> Self {
            Self(OHLC::dummy())
        }

        /// Return clone with modified date
        pub fn date(&self, date: time::Date) -> Self {
            let mut new = self.clone();
            new.0.t = date;
            new
        }
    }

    impl WeeklyOHLC {
        pub fn dummy() -> Self {
            Self(OHLC::dummy())
        }

        /// Return clone with modified date
        pub fn date(&self, date: time::Date) -> Self {
            let mut new = self.clone();
            new.0.t = date;
            new
        }
    }

    impl MonthlyOHLC {
        pub fn dummy() -> Self {
            Self(OHLC::dummy())
        }

        /// Return clone with modified date
        pub fn date(&self, date: time::Date) -> Self {
            let mut new = self.clone();
            new.0.t = date;
            new
        }
    }

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
        let daily = DailyOHLC::from_prices(t, &rc_prices).unwrap().0;
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
        let daily = DailyOHLC::from_prices(t, &rc_prices).unwrap().0;
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
    fn test_daily_ohlc_from_empty_prices() {
        let t = 1635347405599; // WED 2021-10-27;
        let rc_prices = vec![];
        assert!(DailyOHLC::from_prices(t, &rc_prices).is_none());
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
        assert_eq!(ohlc.o, since.0.c);
        assert_eq!(ohlc.h, since.0.c);
        assert_eq!(ohlc.l, since.0.c);
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
    fn test_daily_ohlc_propagate_to_same_day() {
        let date = time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap();
        let daily = DailyOHLC(OHLC {
            t: date,
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let to_date = date!(2021 - 10 - 27); // WED 2021-10-27;
        let dailys = daily.propagate_to(&to_date);
        assert!(dailys.is_empty());
    }

    #[test]
    fn test_daily_ohlc_propagate_to_next_day() {
        let date = time::Date::from_calendar_date(2021, time::Month::October, 26).unwrap();
        let daily = DailyOHLC(OHLC {
            t: date,
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let to_date = date!(2021 - 10 - 27); // WED 2021-10-27;
        let dailys = daily.propagate_to(&to_date);
        assert!(dailys.len() == 1);
        let ohlc = &dailys[0].0;
        assert_eq!(ohlc.t, time::macros::date!(2021 - 10 - 27));
        assert_eq!(ohlc.o, daily.0.c);
        assert_eq!(ohlc.h, daily.0.c);
        assert_eq!(ohlc.l, daily.0.c);
        assert_eq!(ohlc.c, daily.0.c);
    }

    #[test]
    fn test_weekly_ohlc_round_date() {
        // MON 25 OCT 2021 to MON 25 OCT 2021
        assert_eq!(
            WeeklyOHLC::round_date(&date!(2021 - 10 - 25)),
            date!(2021 - 10 - 25)
        );
        // WED 27 OCT 2021 to MON 25 OCT 2021
        assert_eq!(
            WeeklyOHLC::round_date(&date!(2021 - 10 - 27)),
            date!(2021 - 10 - 25)
        );
    }

    #[test]
    fn test_weekly_ohlc_from_daily() {
        // WED 27 October 2021
        let daily = DailyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let weekly = WeeklyOHLC::from_daily(&daily);
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
        assert_eq!(ohlc.o, since.0.c);
        assert_eq!(ohlc.h, since.0.c);
        assert_eq!(ohlc.l, since.0.c);
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
    fn test_weekly_ohlc_propagate_to_same_week() {
        let date = time::Date::from_calendar_date(2021, time::Month::October, 25).unwrap();
        let weekly = WeeklyOHLC(OHLC {
            t: date,
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let to_date = date!(2021 - 10 - 27); // WED 2021-10-27;
        let weeklys = weekly.propagate_to(&to_date);
        assert!(weeklys.is_empty());
    }

    #[test]
    fn test_weekly_ohlc_propagate_to_next_week() {
        let date = time::Date::from_calendar_date(2021, time::Month::October, 25).unwrap();
        let weekly = WeeklyOHLC(OHLC {
            t: date,
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let weeklys = weekly.propagate_to(&date!(2021 - 11 - 02)); // TUE 2021-11-02;
        assert_eq!(weeklys.len(), 1);
        let ohlc = &weeklys[0].0;
        assert_eq!(ohlc.t, date!(2021 - 11 - 01));
        assert_eq!(ohlc.o, weekly.0.c);
        assert_eq!(ohlc.h, weekly.0.c);
        assert_eq!(ohlc.l, weekly.0.c);
        assert_eq!(ohlc.c, weekly.0.c);
    }

    #[test]
    fn test_monthly_ohlc_round_date() {
        assert_eq!(
            MonthlyOHLC::round_date(&date!(2021 - 01 - 25)),
            date!(2021 - 01 - 01)
        );
        assert_eq!(
            MonthlyOHLC::round_date(&date!(2021 - 12 - 27)),
            date!(2021 - 12 - 01)
        );
    }

    #[test]
    fn test_monthly_ohlc_from_daily() {
        // WED 27 October 2021
        let daily = DailyOHLC(OHLC {
            t: time::Date::from_calendar_date(2021, time::Month::October, 27).unwrap(),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let monthly = MonthlyOHLC::from_daily(&daily);
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
        assert_eq!(ohlc.o, since.0.c);
        assert_eq!(ohlc.h, since.0.c);
        assert_eq!(ohlc.l, since.0.c);
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

    #[test]
    fn test_monthly_ohlc_propagate_to_same_month() {
        let monthly = MonthlyOHLC(OHLC {
            t: date!(2021 - 10 - 01),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let to_date = date!(2021 - 10 - 27);
        let monthlys = monthly.propagate_to(&to_date);
        assert!(monthlys.is_empty());
    }

    #[test]
    fn test_monthly_ohlc_propagate_to_next_month() {
        let monthly = MonthlyOHLC(OHLC {
            t: date!(2021 - 10 - 01),
            o: 2,
            h: 5,
            l: 1,
            c: 3,
        });
        let monthlys = monthly.propagate_to(&date!(2021 - 11 - 17));
        assert_eq!(monthlys.len(), 1);
        let ohlc = &monthlys[0].0;
        assert_eq!(ohlc.t, date!(2021 - 11 - 01));
        assert_eq!(ohlc.o, monthly.0.c);
        assert_eq!(ohlc.h, monthly.0.c);
        assert_eq!(ohlc.l, monthly.0.c);
        assert_eq!(ohlc.c, monthly.0.c);
    }
}

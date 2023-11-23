use crate::core::types::CoreData;
use crate::core::types::Timestamp;

use super::types::Action;
use super::types::Batch;
use super::types::TimestampRecord;
use crate::core::types::Header;
use crate::framework::StampedData;

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;

pub(super) struct ParserCache {
    pub(super) last_hourly: TimestampRecord,
    pub(super) last_daily: TimestampRecord,
    pub(super) last_weekly: TimestampRecord,
}

pub(super) struct Parser {
    cache: ParserCache,
}

impl Parser {
    pub fn new(cache: ParserCache) -> Self {
        Self { cache }
    }

    pub(super) fn extract_batch(
        &mut self,
        stamped_data: &StampedData<CoreData>,
    ) -> StampedData<Batch> {
        let header = Header::from(stamped_data);

        let hourly_actions = extract_actions(&header, &mut self.cache.last_hourly, Hourly {});
        let daily_actions = extract_actions(&header, &mut self.cache.last_daily, Daily {});
        let weekly_actions = extract_actions(&header, &mut self.cache.last_weekly, Weekly {});

        // Update cache
        self.cache.last_hourly = hourly_actions.last().unwrap().get_inserted().unwrap();
        self.cache.last_daily = daily_actions.last().unwrap().get_inserted().unwrap();
        self.cache.last_weekly = weekly_actions.last().unwrap().get_inserted().unwrap();

        // Pack new batch
        stamped_data.wrap(Batch {
            hourly: hourly_actions,
            daily: daily_actions,
            weekly: weekly_actions,
        })
    }
}

trait Window {
    /// Convert a timestamp to equivalent window rank since genesis
    fn rank(&self, timestamp: Timestamp) -> i64;

    /// Round timestamp up to next whole unit
    fn ceil(&self, record: &TimestampRecord) -> TimestampRecord;

    /// True if `timestamp` is spot on window boundary
    fn is_final(&self, timestamp: Timestamp) -> bool;

    fn size_ms(&self) -> i64;
}

struct Hourly {}
struct Daily {}
struct Weekly {}

impl Window for Hourly {
    fn rank(&self, timestamp: Timestamp) -> i64 {
        timestamp / HOUR_MS
    }

    /// Round timestamp up to next hour
    fn ceil(&self, record: &TimestampRecord) -> TimestampRecord {
        let rem = record.timestamp % HOUR_MS;
        let timestamp = match rem {
            0 => record.timestamp,
            _ => record.timestamp - rem + HOUR_MS,
        };
        TimestampRecord::new(record.height, timestamp)
    }

    fn is_final(&self, timestamp: Timestamp) -> bool {
        timestamp % HOUR_MS == 0
    }

    fn size_ms(&self) -> i64 {
        HOUR_MS
    }
}

impl Window for Daily {
    fn rank(&self, timestamp: Timestamp) -> i64 {
        timestamp / DAY_MS
    }

    /// Round timestamp up to next day
    fn ceil(&self, record: &TimestampRecord) -> TimestampRecord {
        let rem = record.timestamp % DAY_MS;
        let timestamp = match rem {
            0 => record.timestamp,
            _ => record.timestamp - rem + DAY_MS,
        };
        TimestampRecord::new(record.height, timestamp)
    }

    fn is_final(&self, timestamp: Timestamp) -> bool {
        timestamp % DAY_MS == 0
    }

    fn size_ms(&self) -> i64 {
        DAY_MS
    }
}

impl Window for Weekly {
    fn rank(&self, timestamp: Timestamp) -> i64 {
        (timestamp + DAY_MS * 3) / WEEK_MS
    }

    /// Round timestamp up to next week
    fn ceil(&self, record: &TimestampRecord) -> TimestampRecord {
        let rem = (record.timestamp + DAY_MS * 3) % WEEK_MS;
        let timestamp = match rem {
            0 => record.timestamp,
            _ => record.timestamp - rem + WEEK_MS,
        };
        TimestampRecord::new(record.height, timestamp)
    }

    fn is_final(&self, timestamp: Timestamp) -> bool {
        timestamp % WEEK_MS == 0
    }

    fn size_ms(&self) -> i64 {
        WEEK_MS
    }
}

fn extract_actions(
    header: &Header,
    last_record: &TimestampRecord,
    window: impl Window,
) -> Vec<Action> {
    let mut actions = vec![];

    // Determine rank of new and previous timestamps
    let nb_windows = window.rank(header.timestamp);
    let prev_nb_windows = window.rank(last_record.timestamp);
    let rank_diff = nb_windows - prev_nb_windows;

    // Is last record's timestamp precisely on rounded window?
    let prev_is_final = window.is_final(last_record.timestamp);

    if last_record.height > 0 {
        if rank_diff == 0 && !prev_is_final {
            actions.push(Action::DELETE(last_record.height));
        } else if rank_diff == 1 && !prev_is_final {
            actions.push(Action::UPDATE(window.ceil(last_record)));
        } else {
            assert!(header.timestamp > last_record.timestamp);
            let next_t = match prev_is_final {
                false => {
                    let ceiled = window.ceil(last_record);
                    let t = ceiled.timestamp + window.size_ms();
                    actions.push(Action::UPDATE(ceiled));
                    t
                }
                true => last_record.timestamp + window.size_ms(),
            };
            let height = last_record.height;
            for timestamp in (next_t..header.timestamp).step_by(window.size_ms() as usize) {
                actions.push(Action::INSERT(TimestampRecord::new(height, timestamp)))
            }
        }
    }
    // Finally, add header timestamp
    actions.push(Action::INSERT(TimestampRecord::new(
        header.height,
        header.timestamp,
    )));

    actions
}

#[cfg(test)]
mod tests {
    use crate::workers::timestamps::parsing::DAY_MS;
    use crate::workers::timestamps::parsing::HOUR_MS;
    use crate::workers::timestamps::parsing::WEEK_MS;

    use super::extract_actions;
    use super::Action;
    use super::Daily;
    use super::Header;
    use super::Hourly;
    use super::TimestampRecord;
    use super::Weekly;
    use super::Window;

    #[test]
    fn window_hourly() {
        let w = Hourly {};
        assert_eq!(w.size_ms(), HOUR_MS);
        assert_eq!(
            w.ceil(&TimestampRecord::new(123, 3_601_000)),
            TimestampRecord::new(123, 7_200_000)
        );
        assert_eq!(w.rank(3_599_999), 0);
        assert_eq!(w.rank(3_600_000), 1);
        assert_eq!(w.rank(3_601_000), 1);
    }

    #[test]
    fn window_daily() {
        let w = Daily {};
        assert_eq!(w.size_ms(), DAY_MS);
        assert_eq!(
            w.ceil(&TimestampRecord::new(123, 86_401_000)),
            TimestampRecord::new(123, 86_400_000 * 2)
        );
        assert_eq!(w.rank(86_399_999), 0);
        assert_eq!(w.rank(86_400_000), 1);
        assert_eq!(w.rank(86_401_000), 1);
    }

    #[test]
    fn window_weekly() {
        let w = Weekly {};
        assert_eq!(w.size_ms(), WEEK_MS);
        assert_eq!(
            w.ceil(&TimestampRecord::new(123, 345_599_999)),
            TimestampRecord::new(123, 345_600_000)
        );
        // Sunday 4 JAN 1970 23:59:59.999
        assert_eq!(w.rank(345_599_999), 0);
        // Monday 1970 00:00:00
        assert_eq!(w.rank(345_600_000), 1);
        // Monday 1970 00:00:01
        assert_eq!(w.rank(345_600_001), 1);
    }

    #[test]
    fn same_period_live_live() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5 + 5000,
        };
        let header = Header {
            height: 6,
            timestamp: 86_400_000 * 5 + 6000,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };
        let actions = extract_actions(&header, &last_record, Daily {});
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0], Action::DELETE(last_record.height));
        assert_eq!(
            actions[1],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }

    #[test]
    fn same_period_final_live() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5,
        };
        let header = Header {
            height: 6,
            timestamp: 86_400_000 * 5 + 6000,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };

        let actions = extract_actions(&header, &last_record, Daily {});
        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }

    #[test]
    fn next_period_live_live() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5 + 5000,
        };
        let header = Header {
            height: 6,
            timestamp: 86_400_000 * 6 + 6000,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };
        let expected_update = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let actions = extract_actions(&header, &last_record, Daily {});
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0], Action::UPDATE(expected_update));
        assert_eq!(
            actions[1],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }

    #[test]
    fn next_period_live_final() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5 + 5000,
        };
        let header = Header {
            height: 6,
            timestamp: 86_400_000 * 6,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };
        let expected_update = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let actions = extract_actions(&header, &last_record, Daily {});
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0], Action::UPDATE(expected_update));
        assert_eq!(
            actions[1],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }

    #[test]
    fn next_period_final_final() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5,
        };
        let header = Header {
            height: 6,
            timestamp: 86_400_000 * 6,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };
        let actions = extract_actions(&header, &last_record, Daily {});
        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }

    #[test]
    fn skipped_periods_live_live() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5 + 5000,
        };
        let header = Header {
            height: 6,
            timestamp: 86_400_000 * 8 + 6000,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };
        let update = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let intermediate1 = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 7,
        };
        let intermediate2 = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 8,
        };
        let actions = extract_actions(&header, &last_record, Daily {});
        assert_eq!(actions.len(), 4);
        assert_eq!(actions[0], Action::UPDATE(update));
        assert_eq!(actions[1], Action::INSERT(intermediate1));
        assert_eq!(actions[2], Action::INSERT(intermediate2));
        assert_eq!(
            actions[3],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }

    #[test]
    fn skipped_periods_final_live() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5,
        };
        let header = Header {
            height: 6,
            timestamp: 86_400_000 * 8 + 6000,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };
        let intermediate0 = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let intermediate1 = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 7,
        };
        let intermediate2 = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 8,
        };
        let actions = extract_actions(&header, &last_record, Daily {});
        assert_eq!(actions.len(), 4);
        assert_eq!(actions[0], Action::INSERT(intermediate0));
        assert_eq!(actions[1], Action::INSERT(intermediate1));
        assert_eq!(actions[2], Action::INSERT(intermediate2));
        assert_eq!(
            actions[3],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }

    #[test]
    fn skipped_periods_live_final() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5 + 5000,
        };
        let header = Header {
            height: 6,
            timestamp: 86_400_000 * 8,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };
        let update = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let intermediate = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 7,
        };
        let actions = extract_actions(&header, &last_record, Daily {});
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0], Action::UPDATE(update));
        assert_eq!(actions[1], Action::INSERT(intermediate));
        assert_eq!(
            actions[2],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }

    #[test]
    fn skipped_periods_final_final() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5,
        };
        let header = Header {
            height: 6,
            timestamp: 86_400_000 * 8,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };
        let intermediate1 = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let intermediate2 = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 7,
        };
        let actions = extract_actions(&header, &last_record, Daily {});
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0], Action::INSERT(intermediate1));
        assert_eq!(actions[1], Action::INSERT(intermediate2));
        assert_eq!(
            actions[2],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }

    #[test]
    fn genesis_does_not_get_overwritten() -> () {
        let last_record = TimestampRecord {
            height: 0,
            timestamp: 86_400_000,
        };
        let header = Header {
            height: 1,
            timestamp: 86_400_000 + 120_000,
            header_id: "dummy".to_owned(),
            parent_id: "dummy".to_owned(),
        };
        let actions = extract_actions(&header, &last_record, Weekly {});
        assert_eq!(actions.len(), 1);
        assert_eq!(
            actions[0],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }
}

use crate::core::types::BoxData;
use crate::core::types::CoreData;
use crate::core::types::Head;

use super::types::Action;
use super::types::Batch;
use super::types::MiniHeader;
use super::types::TimestampRecord;

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

    pub(super) fn extract_genesis_batch(&mut self, boxes: &Vec<BoxData>) -> Batch {
        let head = Head::genesis();

        // Get genesis out of first box
        let genesis_timestamp = boxes[0].output_timestamp;

        // Init cache
        self.cache.last_hourly = TimestampRecord::new(0, genesis_timestamp);
        self.cache.last_daily = TimestampRecord::new(0, genesis_timestamp);
        self.cache.last_weekly = TimestampRecord::new(0, genesis_timestamp);

        // Prepare batch
        Batch {
            header: MiniHeader::new(head.height, genesis_timestamp, head.header_id),
            hourly: vec![Action::INSERT(TimestampRecord::new(0, genesis_timestamp))],
            daily: vec![Action::INSERT(TimestampRecord::new(0, genesis_timestamp))],
            weekly: vec![Action::INSERT(TimestampRecord::new(0, genesis_timestamp))],
        }
    }

    pub(super) fn extract_batch(&mut self, data: &CoreData) -> Batch {
        let header = MiniHeader::new(
            data.block.header.height,
            data.block.header.timestamp,
            data.block.header.id.clone(),
        );

        let hourly_actions = extract_actions(&header, &mut self.cache.last_hourly, HOUR_MS);
        let daily_actions = extract_actions(&header, &mut self.cache.last_hourly, DAY_MS);
        let weekly_actions = extract_actions(&header, &mut self.cache.last_hourly, WEEK_MS);

        // Update cache
        self.cache.last_hourly = hourly_actions.last().unwrap().get_inserted().unwrap();
        self.cache.last_daily = daily_actions.last().unwrap().get_inserted().unwrap();
        self.cache.last_weekly = weekly_actions.last().unwrap().get_inserted().unwrap();

        // Pack new batch
        Batch {
            header,
            hourly: hourly_actions,
            daily: daily_actions,
            weekly: weekly_actions,
        }
    }
}

fn extract_actions(
    header: &MiniHeader,
    last_record: &TimestampRecord,
    round_ms: i64,
) -> Vec<Action> {
    let mut actions = vec![];

    // Determine rank of new and previous timestamps
    let nb_hours = header.timestamp / round_ms;
    let prev_nb_hours = last_record.timestamp / round_ms;
    let rank_diff = nb_hours - prev_nb_hours;

    // Is last record's timestamp precisely on rounded hour?
    let prev_is_final = last_record.timestamp % round_ms == 0;

    if rank_diff == 0 && !prev_is_final {
        actions.push(Action::DELETE(last_record.height));
    } else if rank_diff == 1 && !prev_is_final {
        actions.push(Action::UPDATE(last_record.ceil(round_ms)));
    } else {
        assert!(header.timestamp > last_record.timestamp);
        let next_t = match prev_is_final {
            false => {
                let ceiled = last_record.ceil(round_ms);
                let t = ceiled.timestamp + round_ms;
                actions.push(Action::UPDATE(ceiled));
                t
            }
            true => last_record.timestamp + round_ms,
        };
        let height = last_record.height;
        for timestamp in (next_t..header.timestamp).step_by(round_ms as usize) {
            actions.push(Action::INSERT(TimestampRecord::new(height, timestamp)))
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
    use super::extract_actions;
    use super::Action;
    use super::MiniHeader;
    use super::TimestampRecord;
    use super::DAY_MS;

    #[test]
    fn same_period_live_live() -> () {
        let last_record = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 5 + 5000,
        };
        let header = MiniHeader {
            height: 6,
            timestamp: 86_400_000 * 5 + 6000,
            id: "dummy".to_owned(),
        };
        let actions = extract_actions(&header, &last_record, DAY_MS);
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
        let header = MiniHeader {
            height: 6,
            timestamp: 86_400_000 * 5 + 6000,
            id: "dummy".to_owned(),
        };

        let actions = extract_actions(&header, &last_record, DAY_MS);
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
        let header = MiniHeader {
            height: 6,
            timestamp: 86_400_000 * 6 + 6000,
            id: "dummy".to_owned(),
        };
        let expected_update = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let actions = extract_actions(&header, &last_record, DAY_MS);
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
        let header = MiniHeader {
            height: 6,
            timestamp: 86_400_000 * 6,
            id: "dummy".to_owned(),
        };
        let expected_update = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let actions = extract_actions(&header, &last_record, DAY_MS);
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
        let header = MiniHeader {
            height: 6,
            timestamp: 86_400_000 * 6,
            id: "dummy".to_owned(),
        };
        let actions = extract_actions(&header, &last_record, DAY_MS);
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
        let header = MiniHeader {
            height: 6,
            timestamp: 86_400_000 * 8 + 6000,
            id: "dummy".to_owned(),
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
        let actions = extract_actions(&header, &last_record, DAY_MS);
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
        let header = MiniHeader {
            height: 6,
            timestamp: 86_400_000 * 8 + 6000,
            id: "dummy".to_owned(),
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
        let actions = extract_actions(&header, &last_record, DAY_MS);
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
        let header = MiniHeader {
            height: 6,
            timestamp: 86_400_000 * 8,
            id: "dummy".to_owned(),
        };
        let update = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let intermediate = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 7,
        };
        let actions = extract_actions(&header, &last_record, DAY_MS);
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
        let header = MiniHeader {
            height: 6,
            timestamp: 86_400_000 * 8,
            id: "dummy".to_owned(),
        };
        let intermediate1 = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 6,
        };
        let intermediate2 = TimestampRecord {
            height: 5,
            timestamp: 86_400_000 * 7,
        };
        let actions = extract_actions(&header, &last_record, DAY_MS);
        assert_eq!(actions.len(), 3);
        assert_eq!(actions[0], Action::INSERT(intermediate1));
        assert_eq!(actions[1], Action::INSERT(intermediate2));
        assert_eq!(
            actions[2],
            Action::INSERT(TimestampRecord::new(header.height, header.timestamp))
        );
    }
}

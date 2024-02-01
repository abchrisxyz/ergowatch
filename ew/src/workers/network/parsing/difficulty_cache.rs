use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use std::collections::VecDeque;

use super::super::types::Difficulty;
use crate::core::types::Timestamp;

pub(super) struct DifficultyCache(VecDeque<(Timestamp, Difficulty)>);

const ONE_DAY: Timestamp = 86_400_000;

impl DifficultyCache {
    /// Create a new cache holding given entries.
    ///
    /// Older entries outside of a 24h window will be trimmed.
    pub fn new(diffs: Vec<(Timestamp, Difficulty)>) -> Self {
        let mut q = VecDeque::new();
        for tuple in diffs {
            q.push_back(tuple)
        }
        let mut slf = Self(q);
        // Trim for good measure
        slf.trim();
        slf
    }

    /// Adds a new entry and drops any more than 24h behind last
    pub fn push(&mut self, tuple: (Timestamp, Difficulty)) {
        self.0.push_back(tuple);
        self.trim();
    }

    /// Calculate 24h mean hash rate
    pub fn calculate_hash_rate(&self) -> i64 {
        if self.0.is_empty() {
            return 0;
        }
        let diff_sum = self
            .0
            .iter()
            .fold(Decimal::new(0, 0), |acc, (_t, d)| acc + d);
        let time_window = self.0.back().unwrap().0 - self.0.front().unwrap().0;
        // Ensure minimal time window of target block time.
        let time_window_seconds = std::cmp::max(time_window / 1000, 120);
        let hash_rate = diff_sum / Decimal::new(time_window_seconds, 0);
        hash_rate.to_i64().unwrap()
    }

    /// Calculate 24h mean difficulty
    pub fn calculate_daily_mean_difficulty(&self) -> Difficulty {
        if self.0.is_empty() {
            return Decimal::new(0, 0);
        }
        let diff_sum = self
            .0
            .iter()
            .fold(Decimal::new(0, 0), |acc, (_t, d)| acc + d);
        (diff_sum / Decimal::from_usize(self.0.len()).unwrap()).round()
    }

    /// Drop any entries more than 24h prior to latest one.
    fn trim(&mut self) {
        let most_recent_timestamp = match self.0.back() {
            Some((t, _d)) => *t,
            None => return,
        };
        loop {
            if let Some((t, _d)) = self.0.front() {
                if most_recent_timestamp - t <= ONE_DAY {
                    break;
                }
            }
            self.0.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::constants::GENESIS_TIMESTAMP;

    use super::*;

    #[test]
    fn test_push_will_trim_old_entries() {
        let oldest = (GENESIS_TIMESTAMP, Decimal::new(100, 0));
        let newer = (GENESIS_TIMESTAMP + 1000, Decimal::new(200, 0));
        let newest = (GENESIS_TIMESTAMP + 1000 + ONE_DAY, Decimal::new(300, 0));
        let mut cache = DifficultyCache::new(vec![]);
        cache.push(oldest);
        assert_eq!(cache.0.front(), Some(&oldest));
        assert_eq!(cache.0.back(), Some(&oldest));
        cache.push(newer);
        assert_eq!(cache.0.front(), Some(&oldest));
        assert_eq!(cache.0.back(), Some(&newer));
        cache.push(newest);
        assert_eq!(cache.0.front(), Some(&newer));
        assert_eq!(cache.0.back(), Some(&newest));
    }

    #[test]
    fn test_hash_rate_small_window() {
        let one_hour: Timestamp = 3_600_000;
        let cache = DifficultyCache::new(vec![
            (GENESIS_TIMESTAMP, Decimal::new(1165864577531904, 0)),
            (
                GENESIS_TIMESTAMP + one_hour,
                Decimal::new(1129245686366208, 0),
            ),
        ]);
        assert_eq!(cache.calculate_hash_rate(), 637_530_628_860i64)
    }
}

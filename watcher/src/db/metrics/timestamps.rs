use crate::parsing::BlockData;
use log::info;
use postgres::Client;
use postgres::Row;
use postgres::Transaction;

/// Daily/hourly timestamps with corresponding block heights. /// /// Latest block always included. // Number of ms is a day const DAY_MS: i64 = 86400_000; // Number of ms in an hour const HOUR_MS: i64 = 3600_000;
/*
    h10 t1
    h20 t2
    h20 t3
    h50 t4

    - Start with genesis height/timestamp.
    - Following timestamps are rounded at daily/hourly intervals, except for last one
    - If timestamp of a block is a new day/hour
        > update latest record to round timestamp to period ceiling (eg day 3.1 becomes day 4)
        > insert block timestamp as latest record
    - If timestamp of a block is same day as last record:
        > if latest is final, insert as new record. Final means has a rounded timestamp.
        > if latest is live, replace latest
    - If timestamp of a block is multiple intervals ahead:
        > generate series of missing records with last height and rounded timestamps, process as above
        latest_h, ceil(latest)
        latest_h, ceil(latest) + 1
        ...
        block_h, block_timestamp
*/

pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    // Daily
    let mut c = Cursor::daily();
    let target = Fields::from(block);
    c.prepare(cache.daily, target);
    c.execute(tx);
    cache.daily = target;

    // Hourly
    let mut c = Cursor::hourly();
    let target = Fields::from(block);
    c.prepare(cache.hourly, target);
    c.execute(tx);
    cache.hourly = target;
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    cache.daily = Cursor::daily().rollback_to(tx, block.height - 1);
    cache.hourly = Cursor::hourly().rollback_to(tx, block.height - 1);
}

pub fn bootstrap(client: &mut Client, work_mem_kb: u32) -> anyhow::Result<()> {
    if !is_bootstrapped(client) {
        do_bootstrap(client, work_mem_kb)?;
    }
    if !constraints_are_set(client) {
        set_constraints(client);
    }
    Ok(())
}

fn do_bootstrap(client: &mut Client, work_mem_kb: u32) -> anyhow::Result<()> {
    let mut tx = client.transaction().unwrap();

    tx.execute(&format!("set local work_mem = {};", work_mem_kb), &[])?;

    info!("Bootstrapping metrics - daily timestamps");

    // Insert first timestamp if not rounded
    tx.execute(
        "
        insert into mtr.timestamps_daily (height, timestamp)
        select height, timestamp
        from core.headers
        where height = 0;",
        &[],
    )?;

    // Insert all daily timestamps
    tx.execute(
        "
        with ranked as (
            select height
                , timestamp
                , timestamp / $1 as rnk
                , lead(timestamp / $1) over (order by height) as next_rnk
            from core.headers
            order by 1
        )
        insert into mtr.timestamps_daily (height, timestamp)
        select height
            , timestamp - timestamp % $1 + $1
        from ranked
        where next_rnk > rnk
        order by height;",
        &[&Cursor::DAY_MS],
    )?;

    // Latest timestamp
    tx.execute(
        "
        insert into mtr.timestamps_daily (height, timestamp)
        select height, timestamp
        from core.headers
        order by height desc limit 1;",
        &[],
    )?;

    info!("Bootstrapping metrics - hourly timestamps");

    // Insert first hourly timestamp if not rounded
    tx.execute(
        "
        insert into mtr.timestamps_hourly (height, timestamp)
        select height, timestamp
        from core.headers
        where height = 0;",
        &[],
    )?;

    // Insert all hourly timestamps
    tx.execute(
        "
        with ranked as (
            select height
                , timestamp
                , timestamp / $1 as rnk
                , lead(timestamp / $1) over (order by height) as next_rnk
            from core.headers
            order by 1
        )
        insert into mtr.timestamps_hourly (height, timestamp)
        select height
            , timestamp - timestamp % $1 + $1
        from ranked
        where next_rnk > rnk
        order by height;",
        &[&Cursor::HOUR_MS],
    )?;

    // Lastest timestamp
    tx.execute(
        "
        insert into mtr.timestamps_hourly (height, timestamp)
        select height, timestamp
        from core.headers
        order by height desc limit 1;",
        &[],
    )?;

    tx.commit()?;
    Ok(())
}

fn is_bootstrapped(client: &mut Client) -> bool {
    let row = client
        .query_one(
            "select exists(select * from mtr.timestamps_daily limit 1);",
            &[],
        )
        .unwrap();
    row.get(0)
}

fn constraints_are_set(client: &mut Client) -> bool {
    client
        .query_one("select timestamps_constraints_set from mtr._log;", &[])
        .unwrap()
        .get(0)
}

fn set_constraints(client: &mut Client) {
    let statements = vec![
        // Daily
        "alter table mtr.timestamps_daily add primary key(timestamp);",
        "alter table mtr.timestamps_daily alter column height set not null;",
        "alter table mtr.timestamps_daily alter column timestamp set not null;",
        "create index on mtr.timestamps_daily using brin(height);",
        // Hourly
        "alter table mtr.timestamps_hourly add primary key(timestamp);",
        "alter table mtr.timestamps_hourly alter column height set not null;",
        "alter table mtr.timestamps_hourly alter column timestamp set not null;",
        "create index on mtr.timestamps_hourly using brin(height);",
        // Flag
        "update mtr._log set timestamps_constraints_set = TRUE;",
    ];

    let mut tx = client.transaction().unwrap();
    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
    tx.commit().unwrap();
}

struct Cursor {
    /// Millisecond multiple to round to
    round_ms: i64,
    /// Timestamp type specific sql statements
    sql: &'static sql::QuerySet,
    /// Changes to be applied
    actions: Vec<Action>,
}

impl Cursor {
    const DAY_MS: i64 = 86400_000;
    const HOUR_MS: i64 = 3600_000;

    /// New daily cursor
    fn daily() -> Self {
        Self {
            sql: &sql::DAILY_QUERYSET,
            round_ms: Self::DAY_MS,
            actions: vec![],
        }
    }

    /// New hourly cursor
    fn hourly() -> Self {
        Self {
            sql: &sql::HOURLY_QUERYSET,
            round_ms: Self::HOUR_MS,
            actions: vec![],
        }
    }

    /// Determine actions to apply to get from `cached` to `target`
    fn prepare(&mut self, cached: Fields, target: Fields) {
        let rank_diff = target.rank(self.round_ms) - cached.rank(self.round_ms);
        let is_final = cached.is_final(self.round_ms);

        if rank_diff == 0 && !is_final {
            self.actions.push(Action::Delete(cached.height));
        } else if rank_diff == 1 && !is_final {
            self.actions
                .push(Action::Update(cached.ceil(self.round_ms)));
        } else {
            assert!(target.timestamp > cached.timestamp);
            let next_t = match is_final {
                false => {
                    let ceiled = cached.ceil(self.round_ms);
                    self.actions.push(Action::Update(ceiled));
                    ceiled.timestamp + self.round_ms
                }
                true => cached.timestamp + self.round_ms,
            };
            let height = cached.height;
            for timestamp in (next_t..target.timestamp).step_by(self.round_ms as usize) {
                self.actions
                    .push(Action::Insert(Fields { height, timestamp }))
            }
        }
        // Finally, add target timestamp
        self.actions.push(Action::Insert(target));
    }

    /// Apply actions
    fn execute(&self, tx: &mut Transaction) {
        for action in &self.actions {
            match action {
                Action::Insert(fields) => self.insert(tx, fields),
                Action::Update(fields) => self.update(tx, fields),
                Action::Delete(height) => self.delete(tx, height),
            }
        }
    }

    fn insert(&self, tx: &mut Transaction, fields: &Fields) {
        tx.execute(self.sql.insert, &[&fields.height, &fields.timestamp])
            .unwrap();
    }

    fn update(&self, tx: &mut Transaction, fields: &Fields) {
        tx.execute(self.sql.update, &[&fields.height, &fields.timestamp])
            .unwrap();
    }

    fn delete(&self, tx: &mut Transaction, height: &i32) {
        tx.execute(self.sql.delete_at_h, &[height]).unwrap();
    }

    /// Roll back to `height`
    ///
    /// `height`: height of previous block.
    fn rollback_to(&mut self, tx: &mut Transaction, height: i32) -> Fields {
        // Delete all rows with timestamps > previous block's.
        let previous_block_timestamp: i64 = tx
            .query_one(sql::GET_HEADER_TIMESTAMP_AT, &[&(height)])
            .unwrap()
            .get(0);
        tx.execute(self.sql.delete_above_t, &[&previous_block_timestamp])
            .unwrap();
        // Restore previous block's timestamp if needed.
        let fields = Fields {
            height: height,
            timestamp: previous_block_timestamp,
        };
        if !fields.is_final(self.round_ms) {
            self.insert(tx, &fields);
        }
        fields
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Fields {
    height: i32,
    timestamp: i64,
}

impl Fields {
    pub fn new() -> Self {
        Self {
            height: 0,
            timestamp: 0,
        }
    }
    pub fn load_latest_daily(client: &mut Client) -> Self {
        match client
            .query_opt(sql::DAILY_QUERYSET.get_latest, &[])
            .unwrap()
        {
            Some(row) => row.into(),
            None => Self::new(),
        }
    }
    pub fn load_latest_hourly(client: &mut Client) -> Self {
        match client
            .query_opt(sql::HOURLY_QUERYSET.get_latest, &[])
            .unwrap()
        {
            Some(row) => row.into(),
            None => Self::new(),
        }
    }

    /// Round timestamp to next multiple of `round_ms`
    fn ceil(&self, round_ms: i64) -> Self {
        Self {
            height: self.height,
            timestamp: ceil_timestamp(self.timestamp, round_ms),
        }
    }

    /// True if timestamp has rounded timestamp
    fn is_final(&self, round_ms: i64) -> bool {
        self.timestamp % round_ms == 0
    }

    /// Returns number of time period the record's timestamp is in
    fn rank(&self, round_ms: i64) -> i64 {
        self.timestamp / round_ms
    }
}

impl From<&BlockData<'_>> for Fields {
    fn from(block: &BlockData) -> Fields {
        Fields {
            height: block.height,
            timestamp: block.timestamp,
        }
    }
}

impl From<Row> for Fields {
    fn from(row: Row) -> Fields {
        Fields {
            height: row.get(0),
            timestamp: row.get(1),
        }
    }
}

#[derive(Debug, PartialEq)]
enum Action {
    /// Insert new record
    Insert(Fields),
    /// Update timestamp of record with same height
    Update(Fields),
    /// Delete record at height
    Delete(i32),
}

#[derive(Debug)]
pub struct Cache {
    pub daily: Fields,
    pub hourly: Fields,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            daily: Fields::new(),
            hourly: Fields::new(),
        }
    }

    pub fn load(client: &mut Client) -> Self {
        Self {
            daily: Fields::load_latest_daily(client),
            hourly: Fields::load_latest_hourly(client),
        }
    }
}

fn ceil_timestamp(timestamp_ms: i64, round_ms: i64) -> i64 {
    let rem = timestamp_ms % round_ms;
    match rem {
        0 => timestamp_ms,
        _ => timestamp_ms - rem + round_ms,
    }
}

mod sql {

    pub(super) struct QuerySet {
        /// Insert timestamp record
        pub insert: &'static str,
        /// Update timestamp at record's height
        pub update: &'static str,
        /// Delete row at record's height
        pub delete_at_h: &'static str,
        /// Delete rows above timestamp
        pub delete_above_t: &'static str,
        /// Get latest row
        pub get_latest: &'static str,
    }

    pub(super) const DAILY_QUERYSET: QuerySet = QuerySet {
        insert: "insert into mtr.timestamps_daily(height, timestamp) values ($1, $2);",
        update: "update mtr.timestamps_daily set timestamp = $2 where height = $1;",
        delete_at_h: "delete from mtr.timestamps_daily where height = $1;",
        delete_above_t: "delete from mtr.timestamps_daily where timestamp > $1;",
        get_latest: "select height, timestamp from mtr.timestamps_daily order by 1 desc limit 1;",
    };

    pub(super) const HOURLY_QUERYSET: QuerySet = QuerySet {
        insert: "insert into mtr.timestamps_hourly(height, timestamp) values ($1, $2);",
        update: "update mtr.timestamps_hourly set timestamp = $2 where height = $1;",
        delete_at_h: "delete from mtr.timestamps_hourly where height = $1;",
        delete_above_t: "delete from mtr.timestamps_hourly where timestamp > $1;",
        get_latest: "select height, timestamp from mtr.timestamps_hourly order by 1 desc limit 1;",
    };

    pub(super) const GET_HEADER_TIMESTAMP_AT: &'static str =
        "select timestamp from core.headers where height = $1;";
}

#[cfg(test)]
mod tests {
    use super::Action;
    use super::Cursor;
    use super::Fields;
    use pretty_assertions::assert_eq;

    #[test]
    fn same_period_live_live() -> () {
        let latest = Fields {
            height: 5,
            timestamp: 86400_000 * 5 + 5000,
        };
        let target = Fields {
            height: 6,
            timestamp: 86400_000 * 5 + 6000,
        };

        let mut c = Cursor::daily();
        c.prepare(latest, target);
        assert_eq!(c.actions.len(), 2);
        assert_eq!(c.actions[0], Action::Delete(latest.height));
        assert_eq!(c.actions[1], Action::Insert(target));
    }

    #[test]
    fn same_period_final_live() -> () {
        let latest = Fields {
            height: 5,
            timestamp: 86400_000 * 5,
        };
        let target = Fields {
            height: 6,
            timestamp: 86400_000 * 5 + 6000,
        };

        let mut c = Cursor::daily();
        c.prepare(latest, target);
        assert_eq!(c.actions.len(), 1);
        assert_eq!(c.actions[0], Action::Insert(target));
    }

    #[test]
    fn next_period_live_live() -> () {
        let latest = Fields {
            height: 5,
            timestamp: 86400_000 * 5 + 5000,
        };
        let target = Fields {
            height: 6,
            timestamp: 86400_000 * 6 + 6000,
        };
        let update = Fields {
            height: 5,
            timestamp: 86400_000 * 6,
        };
        let mut c = Cursor::daily();
        c.prepare(latest, target);
        assert_eq!(c.actions.len(), 2);
        assert_eq!(c.actions[0], Action::Update(update));
        assert_eq!(c.actions[1], Action::Insert(target));
    }

    #[test]
    fn next_period_live_final() -> () {
        let latest = Fields {
            height: 5,
            timestamp: 86400_000 * 5 + 5000,
        };
        let target = Fields {
            height: 6,
            timestamp: 86400_000 * 6,
        };
        let update = Fields {
            height: 5,
            timestamp: 86400_000 * 6,
        };

        let mut c = Cursor::daily();
        c.prepare(latest, target);
        assert_eq!(c.actions.len(), 2);
        assert_eq!(c.actions[0], Action::Update(update));
        assert_eq!(c.actions[1], Action::Insert(target));
    }

    #[test]
    fn next_period_final_final() -> () {
        let latest = Fields {
            height: 5,
            timestamp: 86400_000 * 5,
        };
        let target = Fields {
            height: 6,
            timestamp: 86400_000 * 6,
        };

        let mut c = Cursor::daily();
        c.prepare(latest, target);
        assert_eq!(c.actions.len(), 1);
        assert_eq!(c.actions[0], Action::Insert(target));
    }

    #[test]
    fn skipped_periods_live_live() -> () {
        let latest = Fields {
            height: 5,
            timestamp: 86400_000 * 5 + 5000,
        };
        let target = Fields {
            height: 6,
            timestamp: 86400_000 * 8 + 6000,
        };
        let update = Fields {
            height: 5,
            timestamp: 86400_000 * 6,
        };
        let intermediate1 = Fields {
            height: 5,
            timestamp: 86400_000 * 7,
        };
        let intermediate2 = Fields {
            height: 5,
            timestamp: 86400_000 * 8,
        };
        let mut c = Cursor::daily();
        c.prepare(latest, target);
        assert_eq!(c.actions.len(), 4);
        assert_eq!(c.actions[0], Action::Update(update));
        assert_eq!(c.actions[1], Action::Insert(intermediate1));
        assert_eq!(c.actions[2], Action::Insert(intermediate2));
        assert_eq!(c.actions[3], Action::Insert(target));
    }

    #[test]
    fn skipped_periods_final_live() -> () {
        let latest = Fields {
            height: 5,
            timestamp: 86400_000 * 5,
        };
        let target = Fields {
            height: 6,
            timestamp: 86400_000 * 8 + 6000,
        };
        let intermediate0 = Fields {
            height: 5,
            timestamp: 86400_000 * 6,
        };
        let intermediate1 = Fields {
            height: 5,
            timestamp: 86400_000 * 7,
        };
        let intermediate2 = Fields {
            height: 5,
            timestamp: 86400_000 * 8,
        };
        let mut c = Cursor::daily();
        c.prepare(latest, target);
        assert_eq!(c.actions.len(), 4);
        assert_eq!(c.actions[0], Action::Insert(intermediate0));
        assert_eq!(c.actions[1], Action::Insert(intermediate1));
        assert_eq!(c.actions[2], Action::Insert(intermediate2));
        assert_eq!(c.actions[3], Action::Insert(target));
    }

    #[test]
    fn skipped_periods_live_final() -> () {
        let latest = Fields {
            height: 5,
            timestamp: 86400_000 * 5 + 5000,
        };
        let target = Fields {
            height: 6,
            timestamp: 86400_000 * 8,
        };
        let update = Fields {
            height: 5,
            timestamp: 86400_000 * 6,
        };
        let intermediate = Fields {
            height: 5,
            timestamp: 86400_000 * 7,
        };
        let mut c = Cursor::daily();
        c.prepare(latest, target);
        assert_eq!(c.actions.len(), 3);
        assert_eq!(c.actions[0], Action::Update(update));
        assert_eq!(c.actions[1], Action::Insert(intermediate));
        assert_eq!(c.actions[2], Action::Insert(target));
    }

    #[test]
    fn skipped_periods_final_final() -> () {
        let latest = Fields {
            height: 5,
            timestamp: 86400_000 * 5,
        };
        let target = Fields {
            height: 6,
            timestamp: 86400_000 * 8,
        };
        let intermediate1 = Fields {
            height: 5,
            timestamp: 86400_000 * 6,
        };
        let intermediate2 = Fields {
            height: 5,
            timestamp: 86400_000 * 7,
        };
        let mut c = Cursor::daily();
        c.prepare(latest, target);
        assert_eq!(c.actions.len(), 3);
        assert_eq!(c.actions[0], Action::Insert(intermediate1));
        assert_eq!(c.actions[1], Action::Insert(intermediate2));
        assert_eq!(c.actions[2], Action::Insert(target));
    }
}

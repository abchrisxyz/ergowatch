use crate::parsing::BlockData;
use postgres::GenericClient;
use postgres::Transaction;

// const DAYS_1W: i32 = 7;
// const DAYS_4W: i32 = 28;
// const DAYS_6M: i32 = 183;
// const DAYS_1Y: i32 = 365;

pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    let rows = tx
        .query(sql::GET_NEXT_HEIGHTS, &[&cache.y1, &block.timestamp])
        .unwrap();
    cache.current = block.height;
    cache.d1 = rows[0].get(0);
    cache.w1 = rows[1].get(0);
    cache.w4 = rows[2].get(0);
    cache.m6 = rows[3].get(0);
    cache.y1 = rows[4].get(0);
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    let prev_height = block.height - 1;
    let rows = tx
        .query(sql::LOAD_WINDOW_HEIGHTS_AT, &[&prev_height])
        .unwrap();
    cache.current = rows[0].get(0);
    cache.d1 = rows[1].get(0);
    cache.w1 = rows[2].get(0);
    cache.w4 = rows[3].get(0);
    cache.m6 = rows[4].get(0);
    cache.y1 = rows[5].get(0);
}

#[derive(Debug)]
pub struct Cache {
    pub current: i32,
    pub d1: i32,
    pub w1: i32,
    pub w4: i32,
    pub m6: i32,
    pub y1: i32,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            current: 0,
            d1: 0,
            w1: 0,
            w4: 0,
            m6: 0,
            y1: 0,
        }
    }

    pub fn load(client: &mut impl GenericClient) -> Self {
        let rows = client.query(sql::LOAD_WINDOW_HEIGHTS, &[]).unwrap();
        if rows.len() == 0 {
            Self::new()
        } else {
            Self {
                current: rows[0].get(0),
                d1: rows[1].get(0),
                w1: rows[2].get(0),
                w4: rows[3].get(0),
                m6: rows[4].get(0),
                y1: rows[5].get(0),
            }
        }
    }
}

mod sql {
    /// Get heights for new block.
    ///
    /// $1: last 1-year height (lower-bound for new heights)
    /// $2: timestamp of current block
    ///
    /// Doesn't yield current height as already known.
    pub(super) const GET_NEXT_HEIGHTS: &'static str = "
        with window_days as (
            select unnest(array[1, 7, 28, 183, 365]) as days
        )
        select h.height
        from window_days w, lateral (
            select height
            from core.headers
            where height >= $1 and timestamp >= $2 - 86400000::bigint * w.days
            order by height
            limit 1
        ) h
        order by 1 desc";

    /// Load latest heights for each time window
    pub(super) const LOAD_WINDOW_HEIGHTS: &'static str = "
        with window_days as (
            select unnest(array[0, 1, 7, 28, 183, 365]) as days
        )
        select h.height
        from window_days w, lateral (
            select height
            from core.headers
            where timestamp >= (
                select timestamp - 86400000::bigint * w.days
                from core.headers
                order by height desc
                limit 1
            )
            order by height
            limit 1
        ) h
        order by 1 desc";

    /// Load heights for given block
    ///
    /// $1: target height
    pub(super) const LOAD_WINDOW_HEIGHTS_AT: &'static str = "
        with window_days as (
            select unnest(array[0, 1, 7, 28, 183, 365]) as days
        )
        select h.height
        from window_days w, lateral (
            select height
            from core.headers
            where timestamp >= (
                select timestamp - 86400000::bigint * w.days
                from core.headers
                where height = $1
                order by height desc
                limit 1
            )
            order by height
            limit 1
        ) h
        order by 1 desc";
}

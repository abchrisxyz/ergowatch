use crate::coingecko::TimeSeries;
use crate::constants::GENESIS_TIMESTAMP;
use postgres::types::Type;
use postgres::Client;
use postgres::Transaction;

pub(super) fn include_timeseries(
    tx: &mut Transaction,
    timeseries: &TimeSeries,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    // Insert into database
    let stmt = tx
        .prepare_typed(
            "insert into cgo.ergusd (timestamp, value) values ($1, $2);",
            &[Type::INT8, Type::FLOAT8],
        )
        .unwrap();
    for price in timeseries {
        tx.execute(&stmt, &[&(price.timestamp_ms as i64), &price.usd])
            .unwrap();
    }
    // Update cache
    if let Some(price) = timeseries.last() {
        cache.last_timestamp = price.timestamp_ms;
    }
    Ok(())
}

#[derive(Debug)]
pub struct Cache {
    /// Timestamp of last fetched price point
    pub(super) last_timestamp: u64,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            last_timestamp: GENESIS_TIMESTAMP,
        }
    }

    pub fn load(client: &mut Client) -> Self {
        let row = client
            .query_one("select max(timestamp) from cgo.ergusd;", &[])
            .unwrap();
        let timestamp: Option<i64> = row.get(0);
        Cache {
            last_timestamp: match timestamp {
                Some(i) => i as u64,
                None => GENESIS_TIMESTAMP,
            },
        }
    }
}

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
        cache.last_datapoint.timestamp = price.timestamp_ms;
        cache.last_datapoint.value = price.usd;
    }
    Ok(())
}

#[derive(Debug)]
pub struct Cache {
    /// Last datapoint fetched from CoinGecko
    pub(super) last_datapoint: DataPoint,
}

#[derive(Debug)]
pub(super) struct DataPoint {
    pub timestamp: u64,
    pub value: f64,
}

impl DataPoint {
    fn new() -> Self {
        Self {
            timestamp: GENESIS_TIMESTAMP - 1,
            value: 0f64,
        }
    }
}

impl Cache {
    pub fn new() -> Self {
        Self {
            last_datapoint: DataPoint::new(),
        }
    }
    pub fn load(client: &mut Client) -> Self {
        let row = client
            .query_opt(
                "select timestamp, value from cgo.ergusd order by 1 desc limit 1;",
                &[],
            )
            .unwrap();
        Self {
            last_datapoint: match row {
                Some(row) => {
                    let t: i64 = row.get(0);
                    DataPoint {
                        timestamp: t as u64,
                        value: row.get(1),
                    }
                }
                None => DataPoint::new(),
            },
        }
    }
}

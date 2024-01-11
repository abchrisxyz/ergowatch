use super::super::types::HourlyRecord;
use crate::core::types::Timestamp;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

pub(super) async fn insert(client: &Client, record: &HourlyRecord) {
    tracing::trace!("insert {record:?}");
    let sql = "insert into coingecko.ergusd_hourly (timestamp, value) values ($1, $2);";
    client
        .execute(sql, &[&record.timestamp, &record.usd])
        .await
        .unwrap();
}

pub(super) async fn insert_many(pgtx: &Transaction<'_>, records: &[HourlyRecord]) {
    tracing::trace!("insert_many {records:?}");
    let sql = format!(
        "
        insert into coingecko.ergusd_hourly (timestamp, value) values {};
    ",
        records
            .iter()
            .map(|r| format!("({}, {})", r.timestamp, r.usd))
            .collect::<Vec<String>>()
            .join(",")
    );

    pgtx.execute(&sql, &[]).await.unwrap();
}

/// Return latest hourly record.
pub(super) async fn get_latest(client: &Client) -> Option<HourlyRecord> {
    let sql = "
        select timestamp
            , value
        from coingecko.ergusd_hourly
        order by 1 desc
        limit 1
    ";
    client.query_opt(sql, &[]).await.unwrap().and_then(|row| {
        Some(HourlyRecord {
            timestamp: row.get(0),
            usd: row.get(1),
        })
    })
}

/// Get last hourly record on or prior to given `timestamp`.
pub(super) async fn get_last_prior_to(
    client: &Client,
    timestamp: Timestamp,
) -> Option<HourlyRecord> {
    tracing::trace!("get_last_prior_to {timestamp}");
    let sql = "
        select timestamp
            , value
        from coingecko.ergusd_hourly
        where timestamp <= $1
        order by timestamp desc
        limit 1;";
    client
        .query_opt(sql, &[&timestamp])
        .await
        .unwrap()
        .and_then(|row| {
            Some(HourlyRecord {
                timestamp: row.get(0),
                usd: row.get(1),
            })
        })
}

/// Get hourly records since given `timestamp`.
pub(super) async fn get_since(client: &Client, timestamp: Timestamp) -> Vec<HourlyRecord> {
    tracing::trace!("get_since {timestamp}");
    let sql = "
        select timestamp
            , value
        from coingecko.ergusd_hourly
        where timestamp >= $1
        order by 1;
    ";
    client
        .query(sql, &[&timestamp])
        .await
        .unwrap()
        .iter()
        .map(|row| HourlyRecord {
            timestamp: row.get(0),
            usd: row.get(1),
        })
        .collect()
}

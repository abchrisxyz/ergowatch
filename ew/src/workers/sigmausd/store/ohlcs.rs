use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::OHLCGroup;
use super::super::types::OHLC;
use crate::core::types::Height;

pub async fn get_latest_group(client: &Client) -> OHLCGroup {
    OHLCGroup {
        daily: get_latest_daily(client).await,
        weekly: get_latest_weekly(client).await,
        monthly: get_latest_monthly(client).await,
    }
}

async fn get_latest_daily(client: &Client) -> OHLC {
    let qry = "
        select t
            , o
            , h
            , l
            , c
        from sigmausd.rc_ohlc_daily order by 1 desc limit 1;
    ";
    let row = client.query_one(qry, &[]).await.unwrap();
    OHLC {
        t: row.get(0),
        o: row.get(1),
        h: row.get(2),
        l: row.get(3),
        c: row.get(4),
    }
}

async fn get_latest_weekly(client: &Client) -> OHLC {
    let qry = "
        select t
            , o
            , h
            , l
            , c
        from sigmausd.rc_ohlc_weekly order by 1 desc limit 1;
    ";
    let row = client.query_one(qry, &[]).await.unwrap();
    OHLC {
        t: row.get(0),
        o: row.get(1),
        h: row.get(2),
        l: row.get(3),
        c: row.get(4),
    }
}

async fn get_latest_monthly(client: &Client) -> OHLC {
    let qry = "
        select t
            , o
            , h
            , l
            , c
        from sigmausd.rc_ohlc_monthly order by 1 desc limit 1;
    ";
    let row = client.query_one(qry, &[]).await.unwrap();
    OHLC {
        t: row.get(0),
        o: row.get(1),
        h: row.get(2),
        l: row.get(3),
        c: row.get(4),
    }
}

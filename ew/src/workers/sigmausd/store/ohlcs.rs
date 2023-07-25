use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::DailyOHLC;
use super::super::types::MonthlyOHLC;
use super::super::types::OHLCGroup;
use super::super::types::WeeklyOHLC;
use super::super::types::OHLC;

pub(super) async fn get_latest_group(client: &Client) -> OHLCGroup {
    OHLCGroup {
        daily: get_latest_daily(client).await,
        weekly: get_latest_weekly(client).await,
        monthly: get_latest_monthly(client).await,
    }
}

async fn get_latest_daily(client: &Client) -> DailyOHLC {
    let qry = "
        select t
            , o
            , h
            , l
            , c
        from sigmausd.rc_ohlc_daily order by 1 desc limit 1;
    ";
    let row = client.query_one(qry, &[]).await.unwrap();
    DailyOHLC(OHLC {
        t: row.get(0),
        o: row.get(1),
        h: row.get(2),
        l: row.get(3),
        c: row.get(4),
    })
}

async fn get_latest_weekly(client: &Client) -> WeeklyOHLC {
    let qry = "
        select t
            , o
            , h
            , l
            , c
        from sigmausd.rc_ohlc_weekly order by 1 desc limit 1;
    ";
    let row = client.query_one(qry, &[]).await.unwrap();
    WeeklyOHLC(OHLC {
        t: row.get(0),
        o: row.get(1),
        h: row.get(2),
        l: row.get(3),
        c: row.get(4),
    })
}

async fn get_latest_monthly(client: &Client) -> MonthlyOHLC {
    let qry = "
        select t
            , o
            , h
            , l
            , c
        from sigmausd.rc_ohlc_monthly order by 1 desc limit 1;
    ";
    let row = client.query_one(qry, &[]).await.unwrap();
    MonthlyOHLC(OHLC {
        t: row.get(0),
        o: row.get(1),
        h: row.get(2),
        l: row.get(3),
        c: row.get(4),
    })
}

pub(super) async fn update_daily(pgtx: &Transaction<'_>, rec: &DailyOHLC) {
    log_daily(pgtx).await;
    let sql = "
        update sigmausd.rc_ohlc_daily
        set t = $1
            , o = $2
            , h = $3
            , l = $4
            , c = $5;
    ";
    pgtx.execute(sql, &[&rec.0.t, &rec.0.o, &rec.0.h, &rec.0.l, &rec.0.c])
        .await
        .unwrap();
}

pub(super) async fn update_weekly(pgtx: &Transaction<'_>, rec: &WeeklyOHLC) {
    log_weekly(pgtx).await;
    let sql = "
        update sigmausd.rc_ohlc_weekly
        set t = $1
            , o = $2
            , h = $3
            , l = $4
            , c = $5;
    ";
    pgtx.execute(sql, &[&rec.0.t, &rec.0.o, &rec.0.h, &rec.0.l, &rec.0.c])
        .await
        .unwrap();
}

pub(super) async fn update_monthly(pgtx: &Transaction<'_>, rec: &MonthlyOHLC) {
    log_monthly(pgtx).await;
    let sql = "
        update sigmausd.rc_ohlc_monthly
        set t = $1
            , o = $2
            , h = $3
            , l = $4
            , c = $5;
    ";
    pgtx.execute(sql, &[&rec.0.t, &rec.0.o, &rec.0.h, &rec.0.l, &rec.0.c])
        .await
        .unwrap();
}

/// Copies current daily ohlc record to log
async fn log_daily(pgtx: &Transaction<'_>) {
    let sql = "
        insert into sigmausd._log_rc_ohlc_daily (t, o, h, l, c)
        select t
            , o
            , h
            , l
            , c
        from sigmausd.rc_ohlc_daily;
    ";
    pgtx.execute(sql, &[]).await.unwrap();
}

/// Copies current weekly ohlc record to log
async fn log_weekly(pgtx: &Transaction<'_>) {
    let sql = "
        insert into sigmausd._log_rc_ohlc_weekly (t, o, h, l, c)
        select t
            , o
            , h
            , l
            , c
        from sigmausd.rc_ohlc_weekly;
    ";
    pgtx.execute(sql, &[]).await.unwrap();
}

/// Copies current monthly ohlc record to log
async fn log_monthly(pgtx: &Transaction<'_>) {
    let sql = "
        insert into sigmausd._log_rc_ohlc_monthly (t, o, h, l, c)
        select t
            , o
            , h
            , l
            , c
        from sigmausd.rc_ohlc_monthly;
    ";
    pgtx.execute(sql, &[]).await.unwrap();
}

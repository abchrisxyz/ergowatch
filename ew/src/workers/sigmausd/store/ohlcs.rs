use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::super::types::DailyOHLC;
use super::super::types::MonthlyOHLC;
use super::super::types::OHLCGroup;
use super::super::types::WeeklyOHLC;
use super::super::types::OHLC;
use crate::core::types::Height;

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

pub(super) async fn upsert_daily(pgtx: &Transaction<'_>, rec: &DailyOHLC, height: Height) {
    // Upsert new record
    let sql = "
        insert into sigmausd.rc_ohlc_daily (t, o, h, l, c)
        values ($1, $2, $3, $4, $5)
        on conflict (t) do update
        set t = $1
            , o = $2
            , h = $3
            , l = $4
            , c = $5;
        ";
    pgtx.execute(sql, &[&rec.0.t, &rec.0.o, &rec.0.h, &rec.0.l, &rec.0.c])
        .await
        .unwrap();

    // Then copy new record to log as well
    let sql = "
        insert into sigmausd._log_rc_ohlc_daily (h, t, o, h, l, c)
        values ($1, $2, $3, $4, $5, $6);
    ";
    pgtx.execute(
        sql,
        &[&height, &rec.0.t, &rec.0.o, &rec.0.h, &rec.0.l, &rec.0.c],
    )
    .await
    .unwrap();
}

pub(super) async fn upsert_weekly(pgtx: &Transaction<'_>, rec: &WeeklyOHLC, height: Height) {
    // Upsert new record
    let sql = "
        insert into sigmausd.rc_ohlc_weekly (t, o, h, l, c)
        values ($1, $2, $3, $4, $5)
        on conflict (t) do update
        set t = $1
            , o = $2
            , h = $3
            , l = $4
            , c = $5;
    ";
    pgtx.execute(sql, &[&rec.0.t, &rec.0.o, &rec.0.h, &rec.0.l, &rec.0.c])
        .await
        .unwrap();

    // Then copy new record to log as well
    let sql = "
        insert into sigmausd._log_rc_ohlc_weekly (h, t, o, h, l, c)
        values ($1, $2, $3, $4, $5, $6);
    ";
    pgtx.execute(
        sql,
        &[&height, &rec.0.t, &rec.0.o, &rec.0.h, &rec.0.l, &rec.0.c],
    )
    .await
    .unwrap();
}

pub(super) async fn upsert_monthly(pgtx: &Transaction<'_>, rec: &MonthlyOHLC, height: Height) {
    // Upsert new record
    let sql = "
        insert into sigmausd.rc_ohlc_monthly (t, o, h, l, c)
        values ($1, $2, $3, $4, $5)
        on conflict (t) do update
        set t = $1
            , o = $2
            , h = $3
            , l = $4
            , c = $5;
    ";
    pgtx.execute(sql, &[&rec.0.t, &rec.0.o, &rec.0.h, &rec.0.l, &rec.0.c])
        .await
        .unwrap();

    // Then copy new record to log as well
    let sql = "
        insert into sigmausd._log_rc_ohlc_monthly (h, t, o, h, l, c)
        values ($1, $2, $3, $4, $5, $6);
    ";
    pgtx.execute(
        sql,
        &[&height, &rec.0.t, &rec.0.o, &rec.0.h, &rec.0.l, &rec.0.c],
    )
    .await
    .unwrap();
}

/// Restores previous known state if current block modified it.
pub(super) async fn roll_back_daily(pgtx: &Transaction<'_>, height: Height) {
    roll_back(pgtx, height, "daily").await;
}

/// Restores previous known state if current block modified it.
pub(super) async fn roll_back_weekly(pgtx: &Transaction<'_>, height: Height) {
    roll_back(pgtx, height, "weekly").await;
}

/// Restores previous known state if current block modified it.
pub(super) async fn roll_back_monthly(pgtx: &Transaction<'_>, height: Height) {
    roll_back(pgtx, height, "monthly").await;
}

async fn roll_back(pgtx: &Transaction<'_>, height: Height, window: &str) {
    // Get log entries for height to roll back
    let sql = format!(
        "
        select t
        from sigmausd._log_rc_ohlc_{window}
        where height = $1;"
    );
    let dates: Vec<time::Date> = pgtx
        .query(&sql, &[&height])
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();

    // delete from ohlc
    for date in &dates {
        let sql = format!(
            "
            delete from sigmausd.rc_ohlc_{window}
            where t = $1;"
        );
        pgtx.execute(&sql, &[date]).await.unwrap();
    }

    // then delete from logs
    let sql = format!(
        "
        delete from sigmausd._log_rc_ohlc_{window}
        where height = $1;"
    );
    pgtx.execute(&sql, &[&height]).await.unwrap();

    // insert back most recent logs
    // conflicts can be ignored as record would be the same
    // new ones get inserted.
    let sql = format!(
        "
        insert into sigmausd.rc_ohlc_{window} (t, o, h, l, c)
        select t, o, h, l, c
        from sigmausd._log_rc_ohlc_{window}
        where height = (select max(height) from sigmausd._log_rc_ohlc_{window})
        on conflict do nothing;"
    );
    pgtx.execute(&sql, &[]).await.unwrap();
}

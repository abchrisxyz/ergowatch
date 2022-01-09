//! # core
//!
//! Read/write access to core tables.

use log::debug;
use postgres::{Client, NoTls};

use crate::types::Header;

pub fn get_height() -> Result<u32, postgres::Error> {
    debug!("Retrieving sync height from db");
    let mut client = Client::connect(
        "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
        NoTls,
    )?;
    let row = client.query_one(
        "select 0 as height union select height from core.headers order by 1 desc limit 1;",
        &[],
    )?;
    let height: i32 = row.get("height");
    Ok(height as u32)
}

pub fn insert_header(header: &Header) -> Result<(), postgres::Error> {
    let mut client = Client::connect(
        "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
        NoTls,
    )?;
    let height: i32 = header.height as i32;
    let timestamp: i64 = header.timestamp as i64;
    client.execute(
        "insert into core.headers (height, id, parent_id, timestamp) values ($1, $2, $3, $4);",
        &[&height, &header.id, &header.parent_id, &timestamp],
    )?;
    Ok(())
}

pub fn delete_header(header: &Header) -> Result<(), postgres::Error> {
    let mut client = Client::connect(
        "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
        NoTls,
    )?;
    let deleted = client.execute("delete from core.headers where id = $1;", &[&header.id])?;
    assert_eq!(deleted, 1);
    Ok(())
}

pub fn get_last_header() -> Result<Option<crate::types::Header>, postgres::Error> {
    let mut client = Client::connect(
        "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
        NoTls,
    )?;
    let row_opt = client.query_opt(
        "select height \
            , id \
            , parent_id \
            , timestamp \
         from core.headers \
         order by 1 desc \
         limit 1;",
        &[],
    )?;
    match row_opt {
        Some(row) => {
            let height: i32 = row.get("height");
            let timestamp: i64 = row.get("timestamp");
            let header = crate::types::Header {
                height: height as u32,
                id: row.get("id"),
                parent_id: row.get("parent_id"),
                timestamp: timestamp as u64,
            };
            Ok(Some(header))
        },
        None => Ok(None)
    }
}

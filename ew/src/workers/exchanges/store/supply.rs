use itertools::Itertools;
use tokio_postgres::GenericClient;
use tokio_postgres::Transaction;

use super::super::types::SupplyDiff;
use super::super::types::SupplyRecord;
use crate::core::types::Height;

pub(super) async fn get_latest(client: &impl GenericClient) -> Option<SupplyRecord> {
    tracing::trace!("get_latest");
    let qry = "
    select height
    , main
    , deposits
    from exchanges.supply
    order by 1 desc
    limit 1;
    ";
    client.query_opt(qry, &[]).await.unwrap().and_then(|row| {
        Some(SupplyRecord {
            height: row.get(0),
            main: row.get(1),
            deposits: row.get(2),
        })
    })
}

pub(super) async fn insert(pgtx: &Transaction<'_>, record: &SupplyRecord) {
    tracing::trace!("insert {record:?}");
    let stmt = "insert into exchanges.supply (height, main, deposits) values ($1, $2, $3);";
    pgtx.execute(stmt, &[&record.height, &record.main, &record.deposits])
        .await
        .unwrap();
}

pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    let sql = "delete from exchanges.supply where height = $1;";
    pgtx.execute(sql, &[&height]).await.unwrap();
}

/*
   Haven't found a simple way to apply patch in one go.
   This naive approach doesn't work:

       ```
       drop schema if exists dev cascade;
       create schema dev;

       create table dev.supply (h int, v int);
       insert into dev.supply(h, v) values (1, 10), (2, 20), (3, 30);

       update dev.supply s
       set v = s.v + p.v
       from (values (2, 5), (3, 1)) as p(h, v)
       where s.h >= p.h;

       select * from dev.supply;
       ```
    yields:

       "h"	"v"
       1	10
       2	25
       3	35 -- should be 36 !!

    Patch must be accumulated at each height first:

        ```
        create schema dev;

        create table dev.supply (h int, v int);
        insert into dev.supply(h, v) values (1, 10), (2, 20), (3, 30), (4, 40);

        update dev.supply s
        set v = s.v + p.v
        from (
            select s as h
                , sum(p.v) over (order by s rows between unbounded preceding and current row) as v
            from generate_series(2, 4) as s
            left join (values (2, 5), (4, 1)) as p(h, v) on p.h = s
        ) p
        where s.h = p.h;

        select * from dev.supply;
        ```
*/
/// Patch deposits supply with given balance diffs series.
pub(super) async fn patch_deposits(pgtx: &Transaction<'_>, patch: &Vec<SupplyDiff>) {
    tracing::trace!("patch_deposits {patch:?}");

    let min_height = patch.iter().min_by_key(|sd| sd.height).unwrap().height;
    let string_patch: String = patch
        .iter()
        .map(|sd| format!("({}, {})", sd.height, sd.nano))
        .join(",");

    let sql = format!("
        update exchanges.supply s
        set deposits = deposits + p.value
        from (
            select series_h as height
                , sum(p.v) over (order by series_h rows between unbounded preceding and current row) as value
            from generate_series($1, (select max(height) from exchanges.supply)) as series_h
            left join (values {}) as p(h, v) on p.h = series_h
        ) p
        where s.height = p.height;", &string_patch);
    pgtx.execute(&sql, &[&min_height]).await.unwrap();
}

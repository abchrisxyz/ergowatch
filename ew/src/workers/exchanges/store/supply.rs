use tokio_postgres::types::Type;
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
   This

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

    Update seems to apply first joined value only.
    For now, applying each diff separately.
*/
/// Patch deposits supply with given balance diffs series.
pub(super) async fn patch_deposits(pgtx: &Transaction<'_>, patch: &Vec<SupplyDiff>) {
    tracing::trace!("patch_deposits {patch:?}");
    let sql = "
        update exchanges.supply
        set deposits = deposits + $1
        where height >= $2;
        ;
    ";
    let stmt = pgtx
        .prepare_typed(sql, &[Type::INT8, Type::INT4])
        .await
        .unwrap();
    for diff in patch {
        pgtx.execute(&stmt, &[&diff.nano, &diff.height])
            .await
            .unwrap();
    }
}

use crate::parsing::BlockData;
use log::info;
use postgres::Client;
use postgres::Transaction;

use super::heights::Cache as HeightsCache;
use super::utils::bootstrap_change_summary;
use super::utils::refresh_change_summary;
use super::Cache;

const SUMMARY_COLUMNS: &[&'static str] = &["daily_1d", "daily_7d", "daily_28d"];

pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &Cache) {
    tx.execute(
        sql::INSERT_SNAPSHOT,
        &[
            &block.height,
            &cache.height_1d_ago,
            &cache.height_7d_ago,
            &cache.height_28d_ago,
        ],
    )
    .unwrap();
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(sql::DELETE_SNAPSHOT, &[&block.height]).unwrap();
}

pub(super) fn refresh_summary(tx: &mut Transaction, hc: &HeightsCache) {
    refresh_change_summary(tx, hc, "mtr.transactions", &SUMMARY_COLUMNS);
}

pub fn bootstrap(client: &mut Client, work_mem_kb: u32) -> anyhow::Result<()> {
    if !is_bootstrapped(client) {
        do_bootstrap(client, work_mem_kb)?;
    }
    if !constraints_are_set(client) {
        set_constraints(client);
    }
    Ok(())
}

fn do_bootstrap(client: &mut Client, work_mem_kb: u32) -> anyhow::Result<()> {
    info!("Bootstrapping metrics - transaction counts");

    let mut tx = client.transaction()?;

    tx.execute(&format!("set local work_mem = {};", work_mem_kb), &[])?;

    // Prep a work table with both height and timestamps
    tx.execute(
        "
        create table tmp_txs as
           select h.height
               , h.timestamp
               , s.tx_count
           from blk.stats s
           join core.headers h on h.height = s.height
           order by 1;
        ",
        &[],
    )?;

    tx.execute("create index on tmp_txs(timestamp);", &[])?;
    tx.execute("alter table mtr.transactions set unlogged;", &[])?;
    tx.execute(
        "
        insert into mtr.transactions (height, daily_1d, daily_7d, daily_28d)
        select height
            , (
                select sum(tx_count)::bigint
                from tmp_txs
                where timestamp > t.timestamp - 86400000::bigint
                    and timestamp <= t.timestamp
            ) as daily_1d
            , (
                select (sum(tx_count) / 7)::bigint
                from tmp_txs
                where timestamp > t.timestamp - (86400000::bigint * 7)
                    and timestamp <= t.timestamp
            ) as daily_7d
            , (
                select (sum(tx_count) / 28)::bigint
                from tmp_txs
                where timestamp > t.timestamp - (86400000::bigint * 28)
                    and timestamp <= t.timestamp
            ) as daily_28d
        from tmp_txs t
        order by 1;
        ",
        &[],
    )?;

    // Summary table
    bootstrap_change_summary(&mut tx, "mtr.transactions", &SUMMARY_COLUMNS);

    tx.execute("alter table mtr.transactions set logged;", &[])?;
    tx.execute("drop table tmp_txs;", &[])?;
    tx.commit()?;
    Ok(())
}

fn is_bootstrapped(client: &mut Client) -> bool {
    client
        .query_one(
            "select exists(select * from mtr.transactions limit 1);",
            &[],
        )
        .unwrap()
        .get(0)
}

fn constraints_are_set(client: &mut Client) -> bool {
    client
        .query_one("select transactions_constraints_set from mtr._log;", &[])
        .unwrap()
        .get(0)
}

fn set_constraints(client: &mut Client) {
    let statements = vec![
        "alter table mtr.transactions add primary key(height);",
        "alter table mtr.transactions alter column height set not null;",
        "alter table mtr.transactions alter column daily_1d set not null;",
        "alter table mtr.transactions alter column daily_7d set not null;",
        "alter table mtr.transactions alter column daily_28d set not null;",
        "update mtr._log set transactions_constraints_set = TRUE;",
    ];
    let mut tx = client.transaction().unwrap();
    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
    tx.commit().unwrap();
}

mod sql {
    pub(super) const INSERT_SNAPSHOT: &str = "
        insert into mtr.transactions (height, daily_1d, daily_7d, daily_28d)
        select $1 as height
            ,  sum(tx_count) filter (where height >= $2) as daily_1d
            , (sum(tx_count) filter (where height >= $3) / 7)::bigint as daily_7d
            , (sum(tx_count) filter (where height >= $4) / 28)::bigint as daily_28d
        from blk.stats;";

    pub(super) const DELETE_SNAPSHOT: &str = "delete from mtr.transactions where height= $1;";
}

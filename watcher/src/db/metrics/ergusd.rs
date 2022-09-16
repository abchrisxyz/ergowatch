/// Erg/USD price defined at each block height.
///
/// Interpolated from hourly CoinGecko data.
use crate::db::coingecko::Cache as CoinGeckoCache;
use crate::parsing::BlockData;
use log::info;
use postgres::Client;
use postgres::Transaction;

/// Assign latest CoinGecko datapoint to block height
pub(super) fn include(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
    cgc: &CoinGeckoCache,
) {
    tx.execute(
        "insert into mtr.ergusd (height, value) values ($1, $2)",
        &[&block.height, &cgc.last_datapoint.value],
    )
    .unwrap();

    tx.execute(
        "insert into mtr.ergusd_provisional (height) values ($1)",
        &[&block.height],
    )
    .unwrap();

    // Update cache
    cache.oldest_provisional_block = get_oldest_provisional_block(tx);
}

/// Remove datapoint for block height
pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    tx.execute(
        "delete from mtr.ergusd where height = $1;",
        &[&block.height],
    )
    .unwrap();

    tx.execute(
        "delete from mtr.ergusd_provisional where height = $1;",
        &[&block.height],
    )
    .unwrap();

    // Update cache
    cache.oldest_provisional_block = get_oldest_provisional_block(tx);
}

/// Check if provisional values can be updated
pub(super) fn pending_update(cache: &Cache, cgc: &CoinGeckoCache) -> bool {
    match &cache.oldest_provisional_block {
        Some(pb) => cgc.last_datapoint.timestamp > pb.timestamp,
        None => false,
    }
}

pub(super) fn update_provisional_values(tx: &mut Transaction, cache: &mut Cache) {
    // Height in the provisional table are guaranteed to be present,
    // in main table as well, so update only, no inserts.
    tx.execute(
        "
        with timestamps as (
            select h.height
                , h.timestamp
                , (select timestamp from cgo.ergusd where timestamp <= h.timestamp order by 1 desc limit 1) as prev_t
                , (select timestamp from cgo.ergusd where timestamp >  h.timestamp order by 1 asc limit 1) as next_t
            from core.headers h
            -- Limit to heights at or after oldest provisional
            where h.height >= (select min(height) from mtr.ergusd_provisional)
        ), new_values as (
            select t.height
                , c1.value + (c2.value - c1.value) * (t.timestamp - t.prev_t) / (t.next_t - t.prev_t) as value
            from timestamps t
            join cgo.ergusd c1 on c1.timestamp = t.prev_t
            join cgo.ergusd c2 on c2.timestamp = t.next_t
        )
        update mtr.ergusd d
        set value = n.value
        from new_values n
        where n.height = d.height;
        ",
        &[],
    ).unwrap();

    // Remove updated heights from provisional table
    tx.execute(
        "
        with updated_heights as (
            select h.height
                , h.timestamp
                , (select timestamp from cgo.ergusd where timestamp <= h.timestamp order by 1 desc limit 1) as prev_t
                , (select timestamp from cgo.ergusd where timestamp >  h.timestamp order by 1 asc limit 1) as next_t
            from core.headers h
            -- Limit to heights at or after oldest provisional
            where h.height >= (select min(height) from mtr.ergusd_provisional)
                -- and limit to heights prior to last CoinGecko datapoint
                and h.timestamp < (select timestamp from cgo.ergusd order by 1 desc limit 1)
        )
        delete from mtr.ergusd_provisional p
        using updated_heights u
        where p.height = u.height;
        ",
        &[],
    )
    .unwrap();

    // Update cache
    cache.oldest_provisional_block = get_oldest_provisional_block(tx);
}

pub(super) fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    if is_bootstrapped(tx) {
        return Ok(());
    }
    info!("Bootstrapping metrics - ergusd");

    // Any blocks before first CoinGecko datapoints
    tx.execute(
        "
        with first_datapoint as (
            select timestamp
                , value
            from cgo.ergusd
            order by timestamp
            limit 1
        )
        insert into mtr.ergusd(height, value)
        select h.height
            , d.value
        from core.headers h, first_datapoint d
        where h.timestamp < d.timestamp
        order by h.height;
        ",
        &[],
    )?;

    // Blocks within two CoinGecko datapoints
    tx.execute(
        "
        with timestamps as (
            select h.height
                , h.timestamp
                , (select timestamp from cgo.ergusd where timestamp <= h.timestamp order by 1 desc limit 1) as prev_t
                , (select timestamp from cgo.ergusd where timestamp >  h.timestamp order by 1 asc limit 1) as next_t
            from core.headers h
        )
        insert into mtr.ergusd(height, value)
            select t.height
                , c1.value + (c2.value - c1.value) * (t.timestamp - t.prev_t) / (t.next_t - t.prev_t) as value
            from timestamps t
            join cgo.ergusd c1 on c1.timestamp = t.prev_t
            join cgo.ergusd c2 on c2.timestamp = t.next_t
            order by 1;
        ",
        &[],
    )?;

    // Most recent blocks, after last CoinGecko datapoint
    tx.execute(
        "
        with last_datapoint as (
            select timestamp
                , value
            from cgo.ergusd
            order by timestamp desc
            limit 1
        )
        insert into mtr.ergusd(height, value)
            select h.height
                , d.value
            from core.headers h, last_datapoint d
            where h.timestamp >= d.timestamp
            order by h.height;
        ",
        &[],
    )?;

    // Add recent blocks without interpolated values to provisional table
    tx.execute(
        "
        with last_datapoint as (
            select timestamp
                , value
            from cgo.ergusd
            order by timestamp desc
            limit 1
        )
        insert into mtr.ergusd_provisional(height)
            select h.height
            from core.headers h, last_datapoint d
            where h.timestamp >= d.timestamp
            order by h.height;
        ",
        &[],
    )?;

    if !constraints_are_set(tx) {
        set_constraints(tx);
    }

    tx.execute("update mtr._log set ergusd_bootstrapped = TRUE;", &[])
        .unwrap();
    Ok(())
}

fn is_bootstrapped(tx: &mut Transaction) -> bool {
    let row = tx
        .query_one("select ergusd_bootstrapped from mtr._log;", &[])
        .unwrap();
    row.get(0)
}

fn constraints_are_set(tx: &mut Transaction) -> bool {
    let row = tx
        .query_one("select ergusd_constraints_set from mtr._log;", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table mtr.ergusd add primary key(height);",
        "alter table mtr.ergusd alter column height set not null;",
        "alter table mtr.ergusd alter column value set not null;",
        "alter table mtr.ergusd_provisional add primary key(height);",
        "alter table mtr.ergusd_provisional alter column height set not null;",
        "update mtr._log set ergusd_constraints_set = TRUE;",
    ];
    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

#[derive(Debug)]
pub struct Cache {
    oldest_provisional_block: Option<ProvisionalBlock>,
}

#[derive(Debug)]
pub struct ProvisionalBlock {
    pub height: i32,
    pub timestamp: u64,
}

impl Cache {
    pub(super) fn new() -> Self {
        Self {
            oldest_provisional_block: None,
        }
    }

    pub(super) fn load(client: &mut Client) -> Self {
        let mut tx = client.transaction().unwrap();
        Self {
            oldest_provisional_block: get_oldest_provisional_block(&mut tx),
        }
    }
}

/// Returns oldest block in provisional table.
fn get_oldest_provisional_block(tx: &mut Transaction) -> Option<ProvisionalBlock> {
    let row = tx
        .query_opt(
            "
            select h.height
                , h.timestamp
            from mtr.ergusd_provisional p
            join core.headers h on h.height = p.height
            order by h.height
            limit 1;",
            &[],
        )
        .unwrap();
    match row {
        Some(row) => {
            let t: i64 = row.get(1);
            Some(ProvisionalBlock {
                height: row.get(0),
                timestamp: t as u64,
            })
        }
        None => None,
    }
}

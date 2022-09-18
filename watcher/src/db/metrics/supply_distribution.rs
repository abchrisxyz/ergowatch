/// Supply distribution
/// Supply on top x % addresses and top x addresses
use crate::db::addresses;
use crate::parsing::BlockData;
use log::info;
use postgres::Client;
use postgres::Row;
use postgres::Transaction;
use std::time::Instant;

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    // Get snapshots
    let p2pk_snapshot: Record = tx.query_one(sql::GET_SNAPSHOT_P2PK, &[]).unwrap().into();
    let cons_snapshot: Record = tx.query_one(sql::GET_SNAPSHOT_CONTRACTS, &[]).unwrap().into();
    let mins_snapshot: Record = tx.query_one(sql::GET_SNAPSHOT_MINERS, &[]).unwrap().into();
    
    // Insert snapshots
    insert_p2pk_record(tx, block.height,  p2pk_snapshot);
    insert_contract_record(tx, block.height,  cons_snapshot);
    insert_miner_record(tx, block.height,  mins_snapshot);
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    // Delete snapshots
    tx.execute(
        "delete from mtr.supply_on_top_addresses_p2pk where height = $1",
        &[&block.height],
    )
    .unwrap();
    tx.execute(
        "delete from mtr.supply_on_top_addresses_contracts where height = $1",
        &[&block.height],
    )
    .unwrap();
    tx.execute(
        "delete from mtr.supply_on_top_addresses_miners where height = $1",
        &[&block.height],
    )
    .unwrap();
}

pub(super) fn repair(tx: &mut Transaction, height: i32) {
    let replay_id = crate::db::repair::REPLAY_ID;

    // Modify queries
    let p2pk_qry = sql::GET_SNAPSHOT_P2PK.replace(" adr.erg ", &format!(" {replay_id}_adr.erg "));
    let cons_qry = sql::GET_SNAPSHOT_CONTRACTS.replace(" adr.erg ", &format!(" {replay_id}_adr.erg "));
    let mins_qry = sql::GET_SNAPSHOT_MINERS.replace(" adr.erg ", &format!(" {replay_id}_adr.erg "));

    // Get snapshots
    let p2pk_snapshot: Record = tx.query_one(&p2pk_qry, &[]).unwrap().into();
    let cons_snapshot: Record = tx.query_one(&cons_qry, &[]).unwrap().into();
    let mins_snapshot: Record = tx.query_one(&mins_qry, &[]).unwrap().into();

    // Update records at h
    update_p2pk_record(tx, height, p2pk_snapshot);
    update_contract_record(tx, height, cons_snapshot);
    update_miner_record(tx, height, mins_snapshot);
}

pub fn bootstrap(client: &mut Client) -> anyhow::Result<()> {
    if !is_bootstrapped(client) {
        do_bootstrap(client)?;
    }
    if !constraints_are_set(client) {
        set_constraints(client);
    }
    Ok(())
}

fn do_bootstrap(client: &mut Client) -> anyhow::Result<()> {
    info!("Bootstrapping metrics - supply distribution");

    let replay_id = "mtr_sd";

    // Retrieve heights to process
    let sync_height = match get_sync_height(client) {
        Some(h) => h,
        None => -1,
    };
    let blocks: Vec<i32> = client
        .query(
            "
            select height
            from core.headers
            where height > $1;",
            &[&sync_height],
        )
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();

    // Prepare replay tables
    let mut tx = client.transaction()?;
    addresses::replay::prepare(&mut tx, sync_height, replay_id);
    tx.commit()?;

    // Prepare query strings
    let p2pk_qry = sql::GET_SNAPSHOT_P2PK.replace(" adr.erg ", &format!(" {replay_id}_adr.erg "));
    let cons_qry = sql::GET_SNAPSHOT_CONTRACTS.replace(" adr.erg ", &format!(" {replay_id}_adr.erg "));
    let mins_qry = sql::GET_SNAPSHOT_MINERS.replace(" adr.erg ", &format!(" {replay_id}_adr.erg "));

    // Bootstrapping will be performed in batches of 1000
    let batch_size = 1000;
    let batches = blocks.chunks(batch_size);
    let nb_batches = batches.len();

    for (ibatch, batch_blocks) in batches.enumerate() {
        let timer = Instant::now();
        let mut tx = client.transaction()?;

        // Prepare statements
        let p2pk_stmt = tx.prepare_typed(p2pk_qry.as_str(), &[])?;
        let cons_stmt = tx.prepare_typed(cons_qry.as_str(), &[])?;
        let mins_stmt = tx.prepare_typed(mins_qry.as_str(), &[])?;

        for height in batch_blocks {
            // step replay
            addresses::replay::step(&mut tx, *height, replay_id);

            // get snapshots
            let p2pk_snapshot: Record = tx.query_one(&p2pk_stmt, &[])?.into();
            let cons_snapshot: Record = tx.query_one(&cons_stmt, &[])?.into();
            let mins_snapshot: Record = tx.query_one(&mins_stmt, &[])?.into();

            // insert records
            insert_p2pk_record(&mut tx, *height, p2pk_snapshot);
            insert_contract_record(&mut tx, *height, cons_snapshot);
            insert_miner_record(&mut tx, *height, mins_snapshot);
        }

        tx.commit()?;

        info!(
            "Bootstrapping distribution metrics - batch {} / {} (processed in {}s)",
            ibatch + 1,
            nb_batches,
            timer.elapsed().as_secs()
        );
    }

    // Cleanup replay tables
    let mut tx = client.transaction()?;
    addresses::replay::cleanup(&mut tx, replay_id);
    tx.commit()?;

    client.execute(
        "update mtr._log set address_counts_bootstrapped = TRUE;",
        &[],
    )?;

    Ok(())
}

fn is_bootstrapped(client: &mut Client) -> bool {
    let row = client
        .query_one("select supply_distribution_bootstrapped from mtr._log;", &[])
        .unwrap();
    row.get(0)
}

fn constraints_are_set(client: &mut Client) -> bool {
    let row = client
        .query_one("select supply_distribution_constraints_set from mtr._log;", &[])
        .unwrap();
    row.get(0)
}

fn get_sync_height(client: &mut Client) -> Option<i32> {
    // P2PK and other tables are progressed in sync, so enough to probe only one.
    let row = client
        .query_one(
            "select max(height) from mtr.supply_on_top_addresses_p2pk;",
            &[],
        )
        .unwrap();
    row.get(0)
}

fn set_constraints(client: &mut Client) {
    let statements = vec![
        // P2PK
        "alter table mtr.supply_on_top_addresses_p2pk add primary key(height);",
        "alter table mtr.supply_on_top_addresses_p2pk alter column height set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column total set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_10_prc set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_1_prc set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_0p1_prc set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_0p01_prc set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_10k set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_1k set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_100 set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_10 set not null;",
        // Contracts
        "alter table mtr.supply_on_top_addresses_contracts add primary key(height);",
        "alter table mtr.supply_on_top_addresses_contracts alter column height set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column total set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_10_prc set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_1_prc set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_0p1_prc set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_0p01_prc set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_10k set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_1k set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_100 set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_10 set not null;",
        // Miners
        "alter table mtr.supply_on_top_addresses_miners add primary key(height);",
        "alter table mtr.supply_on_top_addresses_miners alter column height set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column total set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column top_10_prc set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column top_1_prc set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column top_0p1_prc set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column top_0p01_prc set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column top_10k set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column top_1k set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column top_100 set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column top_10 set not null;",
        // Flag
        "update mtr._log set supply_distribution_constraints_set = TRUE;",
    ];
    let mut tx = client.transaction().unwrap();
    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
    tx.commit().unwrap();
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Record {
    total: i64,
	top_10_prc: i64,
	top_1_prc: i64,
	top_0p1_prc: i64,
	top_0p01_prc: i64,
	top_10k: i64,
	top_1k: i64,
	top_100: i64,
	top_10: i64,
}

impl From<Row> for Record {
    fn from(row: Row) -> Record {
        Record {
            total: row.get(0),
            top_10_prc: row.get(1),
            top_1_prc: row.get(2),
            top_0p1_prc: row.get(3),
            top_0p01_prc: row.get(4),
            top_10k: row.get(5),
            top_1k: row.get(6),
            top_100: row.get(7),
            top_10: row.get(8),
        }
    }
}

fn insert_p2pk_record(tx: &mut Transaction, height: i32, rec: Record) {
    let table = "mtr.supply_on_top_addresses_p2pk";
    insert_record(tx, height, rec, table);
}

fn insert_contract_record(tx: &mut Transaction, height: i32, rec: Record) {
    let table = "mtr.supply_on_top_addresses_contracts";
    insert_record(tx, height, rec, table);
}

fn insert_miner_record(tx: &mut Transaction, height: i32, rec: Record) {
    let table = "mtr.supply_on_top_addresses_miners";
    insert_record(tx, height, rec, table);
}

fn insert_record(tx: &mut Transaction, height: i32, rec: Record, table: &str) {
    tx.execute(
        &format!(
            "
            insert into {table} (
                height,
                total,
                top_10_prc,
                top_1_prc,
                top_0p1_prc,
                top_0p01_prc,
                top_10k,
                top_1k,
                top_100,
                top_10
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);
        "
        ),
        &[
            &height,
            &rec.total,
            &rec.top_10_prc,
            &rec.top_1_prc,
            &rec.top_0p1_prc,
            &rec.top_0p01_prc,
            &rec.top_10k,
            &rec.top_1k,
            &rec.top_100,
            &rec.top_10,
        ],
    )
    .unwrap();
}

fn update_p2pk_record(tx: &mut Transaction, height: i32, rec: Record) {
    let table = "mtr.supply_on_top_addresses_p2pk";
    update_record(tx, height, rec, table);
}

fn update_contract_record(tx: &mut Transaction, height: i32, rec: Record) {
    let table = "mtr.supply_on_top_addresses_contracts";
    update_record(tx, height, rec, table);
}

fn update_miner_record(tx: &mut Transaction, height: i32, rec: Record) {
    let table = "mtr.supply_on_top_addresses_miners";
    update_record(tx, height, rec, table);
}

fn update_record(tx: &mut Transaction, height: i32, rec: Record, table: &str) {
    tx.execute(
        &format!(
            "
            update {table}
            set total = $2
                , top_10_prc = $3
                , top_1_prc = $4
                , top_0p1_prc = $5
                , top_0p01_prc = $6
                , top_10k = $7
                , top_1k = $8
                , top_100 = $9
                , top_10 = $10
            where height = $1;"),
            &[
                &height,
                &rec.total,
                &rec.top_10_prc,
                &rec.top_1_prc,
                &rec.top_0p1_prc,
                &rec.top_0p01_prc,
                &rec.top_10k,
                &rec.top_1k,
                &rec.top_100,
                &rec.top_10,
            ],
    )
    .unwrap();
}
    
mod sql {
    pub(super) const GET_SNAPSHOT_P2PK: &str = 
        "
        with ranked_addresses as (
            select row_number() over (order by value desc) as value_rank
                , value
            from adr.erg b
            join core.addresses a on a.id = b.address_id
            left join cex.addresses c on c.address_id = b.address_id and c.type = 'main'
            where a.p2pk
                -- ignore main cex addresses
                and c.address_id is null
            order by value desc
        ), percentile_ranks as (
            select greatest(1, round(count(*) * 0.1)) as p10
                , greatest(1, round(count(*) * 0.01)) as p1
                , greatest(1, round(count(*) * 0.001)) as p01
                , greatest(1, round(count(*) * 0.0001)) as p001
            from ranked_addresses
        )
        select coalesce((select sum(value) from ranked_addresses), 0)::bigint as total
            
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p10), 0)::bigint as p10
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p1), 0)::bigint as p1
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p01), 0)::bigint as p01
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p001), 0)::bigint as p001

            , coalesce((select sum(value) from ranked_addresses where value_rank <= 10000), 0)::bigint as t10k
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 1000), 0)::bigint as t1k
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 100), 0)::bigint as t100
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 10), 0)::bigint as t10;
        ";

    pub(super) const GET_SNAPSHOT_CONTRACTS: &str = 
        "
        with ranked_addresses as (
            select row_number() over (order by value desc) as value_rank
                , value
            from adr.erg b
            join core.addresses a on a.id = b.address_id
            where not a.p2pk
                and not a.miner
                -- exclude emission contracts
                and b.address_id <> core.address_id('2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU')
                and b.address_id <> coalesce(core.address_id('22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77'), 0)
                and b.address_id <> coalesce(core.address_id('6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p'), 0)
                -- exclude treasury contract
                and b.address_id <> coalesce(core.address_id('4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy'), 0)
            order by value desc
        ), percentile_ranks as (
            select greatest(1, round(count(*) * 0.1)) as p10
                , greatest(1, round(count(*) * 0.01)) as p1
                , greatest(1, round(count(*) * 0.001)) as p01
                , greatest(1, round(count(*) * 0.0001)) as p001
            from ranked_addresses
        )
        select coalesce((select sum(value) from ranked_addresses), 0)::bigint as total
            
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p10), 0)::bigint as p10
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p1), 0)::bigint as p1
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p01), 0)::bigint as p01
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p001), 0)::bigint as p001

            , coalesce((select sum(value) from ranked_addresses where value_rank <= 10000), 0)::bigint as t10k
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 1000), 0)::bigint as t1k
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 100), 0)::bigint as t100
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 10), 0)::bigint as t10;
        ";

    pub(super) const GET_SNAPSHOT_MINERS: &str = 
        "
        with ranked_addresses as (
            select row_number() over (order by value desc) as value_rank
                , value
            from adr.erg b
            join core.addresses a on a.id = b.address_id
            where a.miner
            order by value desc
        ), percentile_ranks as (
            select greatest(1, round(count(*) * 0.1)) as p10
                , greatest(1, round(count(*) * 0.01)) as p1
                , greatest(1, round(count(*) * 0.001)) as p01
                , greatest(1, round(count(*) * 0.0001)) as p001
            from ranked_addresses
        )
        select coalesce((select sum(value) from ranked_addresses), 0)::bigint as total
            
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p10), 0)::bigint as p10
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p1), 0)::bigint as p1
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p01), 0)::bigint as p01
            , coalesce((select sum(r.value) from ranked_addresses r, percentile_ranks p where value_rank <= p.p001), 0)::bigint as p001

            , coalesce((select sum(value) from ranked_addresses where value_rank <= 10000), 0)::bigint as t10k
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 1000), 0)::bigint as t1k
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 100), 0)::bigint as t100
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 10), 0)::bigint as t10;
        ";
}	

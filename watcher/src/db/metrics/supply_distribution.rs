/// Supply distribution
/// Supply on top x % addresses and top x addresses
use crate::db::addresses;
use crate::parsing::BlockData;
use log::info;
use postgres::Client;
use postgres::Row;
use postgres::Transaction;
// use postgres::types::Type;
use std::time::Instant;

use super::address_counts::Cache as AddressCountsCache;

pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &AddressCountsCache) {
    // Calculate rank of 1st percentiles
    let p2pk_1prc = first_percentile_rank(cache.p2pk_counts.total);
    let cons_1prc = first_percentile_rank(cache.contract_counts.total);
    let mins_1prc = first_percentile_rank(cache.miner_counts.total);

    // Get snapshots
    let p2pk_snapshot: Record = tx.query_one(sql::GET_SNAPSHOT_P2PK, &[&p2pk_1prc]).unwrap().into();
    let cons_snapshot: Record = tx.query_one(sql::GET_SNAPSHOT_CONTRACTS, &[&cons_1prc]).unwrap().into();
    let mins_snapshot: Record = tx.query_one(sql::GET_SNAPSHOT_MINERS, &[&mins_1prc]).unwrap().into();
    
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

    // Obtain total address counts and calculate rank of 1st percentiles
    let row = tx.query_one("
        select 
            (select total from mtr.address_counts_by_balance_p2pk where height = $1),
            (select total from mtr.address_counts_by_balance_contracts where height = $1),
            (select total from mtr.address_counts_by_balance_miners where height = $1)
        ", &[&height]).unwrap();
    let p2pk_1prc = first_percentile_rank(row.get(0));
    let cons_1prc = first_percentile_rank(row.get(1));
    let mins_1prc = first_percentile_rank(row.get(2));

    // Get snapshots
    let p2pk_snapshot: Record = tx.query_one(&p2pk_qry, &[&p2pk_1prc]).unwrap().into();
    let cons_snapshot: Record = tx.query_one(&cons_qry, &[&cons_1prc]).unwrap().into();
    let mins_snapshot: Record = tx.query_one(&mins_qry, &[&mins_1prc]).unwrap().into();

    // Update records at h
    update_p2pk_record(tx, height, p2pk_snapshot);
    update_contract_record(tx, height, cons_snapshot);
    update_miner_record(tx, height, mins_snapshot);
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

    let timer = Instant::now();
    let mut tx = client.transaction()?;

    // Prepare replay tables
    addresses::replay::cleanup(&mut tx, replay_id);
    addresses::replay::prepare(&mut tx, sync_height, replay_id);

    // Create top balance work tables
    tx.execute("drop table if exists mtr._top_address_balances_p2pk;", &[])?;
    tx.execute("drop table if exists mtr._top_address_balances_contracts;", &[])?;
    tx.execute("drop table if exists mtr._top_address_balances_miners;", &[])?;
    tx.execute("create table mtr._top_address_balances_p2pk (address_id bigint primary key, value bigint);", &[])?;
    tx.execute("create table mtr._top_address_balances_contracts (address_id bigint primary key, value bigint);", &[])?;
    tx.execute("create table mtr._top_address_balances_miners (address_id bigint primary key, value bigint);", &[])?;
    tx.execute("create index on mtr._top_address_balances_p2pk (value);", &[])?;
    tx.execute("create index on mtr._top_address_balances_contracts (value);", &[])?;
    tx.execute("create index on mtr._top_address_balances_miners (value);", &[])?;

    // Obtain total address counts and calculate rank of 1st percentiles
    let row = tx.query_one("
        select 
            coalesce((select total from mtr.address_counts_by_balance_p2pk where height = $1), 0),
            coalesce((select total from mtr.address_counts_by_balance_contracts where height = $1), 0),
            coalesce((select total from mtr.address_counts_by_balance_miners where height = $1), 0)
        ", &[&sync_height]).unwrap();
    let p2pk_1prc = first_percentile_rank(row.get(0));
    let cons_1prc = first_percentile_rank(row.get(1));
    let mins_1prc = first_percentile_rank(row.get(2));
    
    // Calculate target sizes
    let min_size_p2pk = std::cmp::max(1000, p2pk_1prc);
    let min_size_cons = std::cmp::max(1000, cons_1prc);
    let min_size_mins = std::cmp::max(1000, mins_1prc);
    
    let target_size_p2pk = min_size_p2pk * 2;
    let target_size_cons = min_size_cons * 2;
    let target_size_mins = min_size_mins * 2;
    
    // Populate top balance work tables
    tx.execute("truncate table mtr._top_address_balances_p2pk", &[]).unwrap();
    tx.execute("truncate table mtr._top_address_balances_contracts", &[]).unwrap();
    tx.execute("truncate table mtr._top_address_balances_miners", &[]).unwrap();
    tx.execute(sql_bs::GENERATE_TOP_BALANCES_P2PK, &[&target_size_p2pk]).unwrap();
    tx.execute(sql_bs::GENERATE_TOP_BALANCES_CONTRACTS, &[&target_size_cons]).unwrap();
    tx.execute(sql_bs::GENERATE_TOP_BALANCES_MINERS, &[&target_size_mins]).unwrap();
       
    tx.commit()?;

    info!("Replay preparation: {}s", timer.elapsed().as_secs_f32());

    // Bootstrapping will be performed in batches of 1000
    let batch_size = 1000;
    let batches = blocks.chunks(batch_size);
    let nb_batches = batches.len();

    for (ibatch, batch_blocks) in batches.enumerate() {
        let timer = Instant::now();
        let mut tx = client.transaction()?;

        tx.execute(&format!("set local work_mem = {};", work_mem_kb),&[])?;

        // let t_prep = Instant::now();
        // Prepare statements
        // let p2pk_stmt = tx.prepare_typed(sql_bootstrap::GET_SNAPSHOT_P2PK, &[Type::INT8])?;
        // let cons_stmt = tx.prepare_typed(sql_bootstrap::GET_SNAPSHOT_CONTRACTS, &[Type::INT8])?;
        // let mins_stmt = tx.prepare_typed(sql_bootstrap::GET_SNAPSHOT_MINERS, &[Type::INT8])?;
        // let counts_stmt = tx.prepare_typed("
        //     select 
        //         (select total from mtr.address_counts_by_balance_p2pk where height = $1),
        //         (select total from mtr.address_counts_by_balance_contracts where height = $1),
        //         (select total from mtr.address_counts_by_balance_miners where height = $1)
        //     ", &[Type::INT4])?;
        // let s_prep = t_prep.elapsed().as_secs_f64();

        let mut s_step = 0f64;
        let mut s_updt = 0f64;
        let mut s_prun = 0f64;
        let mut cnt_trm_p = 0;
        let mut cnt_gen_p = 0;
        let mut cnt_trm_c = 0;
        let mut cnt_gen_c = 0;
        let mut cnt_trm_m = 0;
        let mut cnt_gen_m = 0;
        // let mut s_cnts = 0f64;
        let mut s_res = 0f64;
        let mut s_ins = 0f64;
        for height in batch_blocks {
            // step replay
            let t = Instant::now();
            addresses::replay::step(&mut tx, *height, replay_id);
            s_step += t.elapsed().as_secs_f64();

            // Update top balance tables
            let t = Instant::now();
            tx.execute(sql_bs::UPDATE_TOP_BALANCES_P2PK, &[&height]).unwrap();
            tx.execute(sql_bs::UPDATE_TOP_BALANCES_CONTRACTS, &[&height]).unwrap();
            tx.execute(sql_bs::UPDATE_TOP_BALANCES_MINERS, &[&height]).unwrap();
            s_updt += t.elapsed().as_secs_f64();
            
            // Remove spent addresses from top balance tables
            let t = Instant::now();
            tx.execute(sql_bs::PRUNE_TOP_BALANCES_P2PK, &[]).unwrap();
            tx.execute(sql_bs::PRUNE_TOP_BALANCES_CONTRACTS, &[]).unwrap();
            tx.execute(sql_bs::PRUNE_TOP_BALANCES_MINERS, &[]).unwrap();
            s_prun += t.elapsed().as_secs_f64();

            // Obtain total address counts and calculate rank of 1st percentiles
            let row = tx.query_one("
                select 
                    coalesce((select total from mtr.address_counts_by_balance_p2pk where height = $1), 0),
                    coalesce((select total from mtr.address_counts_by_balance_contracts where height = $1), 0),
                    coalesce((select total from mtr.address_counts_by_balance_miners where height = $1), 0)
                ", &[&height]).unwrap();
            let naddr_p2pk: i64 = row.get(0);
            let naddr_cons: i64 = row.get(1);
            let naddr_mins: i64 = row.get(2);
            
            // Calculate 1st percentile counts
            let p2pk_1prc = first_percentile_rank(naddr_p2pk);
            let cons_1prc = first_percentile_rank(naddr_cons);
            let mins_1prc = first_percentile_rank(naddr_mins);

            let min_size_p2pk = std::cmp::max(1000, p2pk_1prc);
            let min_size_cons = std::cmp::max(1000, cons_1prc);
            let min_size_mins = std::cmp::max(1000, mins_1prc);

            let target_size_p2pk = min_size_p2pk * 2;
            let target_size_cons = min_size_cons * 2;
            let target_size_mins = min_size_mins * 2;
            
            let max_size_p2pk = min_size_p2pk * 4;
            let max_size_cons = min_size_cons * 4;
            let max_size_mins = min_size_mins * 4;

            // Get top tables size
            let nrows_p2pk: i64 = tx.query_one(sql_bs::GET_SIZE_P2PK, &[]).unwrap().get(0);
            let nrows_cons: i64 = tx.query_one(sql_bs::GET_SIZE_CONTRACTS, &[]).unwrap().get(0);
            let nrows_mins: i64 = tx.query_one(sql_bs::GET_SIZE_MINERS, &[]).unwrap().get(0);

            // Adjust size if needed
            let t = Instant::now();
            if nrows_p2pk < min_size_p2pk && naddr_p2pk > nrows_p2pk {
                // Regenerate
                tx.execute("truncate table mtr._top_address_balances_p2pk", &[]).unwrap();
                tx.execute(sql_bs::GENERATE_TOP_BALANCES_P2PK, &[&target_size_p2pk]).unwrap();
                cnt_gen_p += 1;
            } else if nrows_p2pk > max_size_p2pk {
                // Trim
                tx.execute(sql_bs::TRIM_TOP_BALANCES_P2PK, &[&target_size_p2pk]).unwrap();
                cnt_trm_p += 1;
            }
            
            if nrows_cons < min_size_cons && naddr_cons > nrows_cons {
                // Regenerate
                tx.execute("truncate table mtr._top_address_balances_contracts", &[]).unwrap();
                tx.execute(sql_bs::GENERATE_TOP_BALANCES_CONTRACTS, &[&target_size_cons]).unwrap();
                cnt_gen_c += 1;
            } else if nrows_cons > max_size_cons {
                // Trim
                tx.execute(sql_bs::TRIM_TOP_BALANCES_CONTRACTS, &[&target_size_cons]).unwrap();
                cnt_trm_c += 1;
            }
        
            if nrows_mins < min_size_mins && naddr_mins > nrows_mins {
                // Regenerate
                tx.execute("truncate table mtr._top_address_balances_miners", &[]).unwrap();
                tx.execute(sql_bs::GENERATE_TOP_BALANCES_MINERS, &[&target_size_mins]).unwrap();
                cnt_gen_m += 1;
            } else if nrows_mins > max_size_mins {
                // Trim
                tx.execute(sql_bs::TRIM_TOP_BALANCES_MINERS, &[&target_size_mins]).unwrap();
                cnt_trm_m += 1;
            }

            s_res += t.elapsed().as_secs_f64();

            // Get snapshots
            let p2pk_snapshot: Record = tx.query_one(sql_bs::GET_SNAPSHOT_P2PK, &[&p2pk_1prc]).unwrap().into();
            let cons_snapshot: Record = tx.query_one(sql_bs::GET_SNAPSHOT_CONTRACTS, &[&cons_1prc]).unwrap().into();
            let mins_snapshot: Record = tx.query_one(sql_bs::GET_SNAPSHOT_MINERS, &[&mins_1prc]).unwrap().into();
            
            // Insert snapshots
            let t = Instant::now();
            insert_p2pk_record(&mut tx, *height,  p2pk_snapshot);
            insert_contract_record(&mut tx, *height,  cons_snapshot);
            insert_miner_record(&mut tx, *height,  mins_snapshot);
            s_ins += t.elapsed().as_secs_f64();
        }

        tx.commit()?;

        let total = timer.elapsed().as_secs_f64();
        info!(
            "Bootstrapping distribution metrics - batch {} / {} (processed in {:.1}s)",
            ibatch + 1,
            nb_batches,
            total
        );
        info!("Total: {:.2}", total);
        info!("Step: {:.2}% ({:.1}s)", s_step / total * 100f64, s_step);
        info!("Updt: {:.2}% ({:.1}s)", s_updt / total * 100f64, s_updt);
        info!("Prun: {:.2}% ({:.1}s)", s_prun / total * 100f64, s_prun);
        info!("Resz: {:.2}% ({:.1}s)", s_res / total * 100f64, s_res);
        info!("Inss: {:.2}% ({:.1}s)", s_ins / total * 100f64, s_ins);
        info!("Regens: {} {} {} | Trims: {} {} {}", cnt_gen_p, cnt_gen_c, cnt_gen_m, cnt_trm_p, cnt_trm_c, cnt_trm_m);
    }

    // Cleanup replay and work tables
    let mut tx = client.transaction()?;
    addresses::replay::cleanup(&mut tx, replay_id);
    
    tx.execute("drop table mtr._top_address_balances_p2pk;", &[])?;
    tx.execute("drop table mtr._top_address_balances_contracts;", &[])?;
    tx.execute("drop table mtr._top_address_balances_miners;", &[])?;
    tx.commit()?;
    
    client.execute(
        "update mtr._log set supply_distribution_bootstrapped = TRUE;",
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
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_1_prc set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_1k set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_100 set not null;",
        "alter table mtr.supply_on_top_addresses_p2pk alter column top_10 set not null;",
        // Contracts
        "alter table mtr.supply_on_top_addresses_contracts add primary key(height);",
        "alter table mtr.supply_on_top_addresses_contracts alter column height set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_1_prc set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_1k set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_100 set not null;",
        "alter table mtr.supply_on_top_addresses_contracts alter column top_10 set not null;",
        // Miners
        "alter table mtr.supply_on_top_addresses_miners add primary key(height);",
        "alter table mtr.supply_on_top_addresses_miners alter column height set not null;",
        "alter table mtr.supply_on_top_addresses_miners alter column top_1_prc set not null;",
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

/// Returns rank of 1st percentile record for given total `count`.
fn first_percentile_rank(count: i64) -> i64 {
    std::cmp::max(1, (count as f64 / 100f64).round() as i64)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Record {
	top_1_prc: i64,
	top_1k: i64,
	top_100: i64,
	top_10: i64,
}

impl From<Row> for Record {
    fn from(row: Row) -> Record {
        Record {
            top_1_prc: row.get(0),
            top_1k: row.get(1),
            top_100: row.get(2),
            top_10: row.get(3),
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
                top_1_prc,
                top_1k,
                top_100,
                top_10
            ) values ($1, $2, $3, $4, $5);
        "
        ),
        &[
            &height,
            &rec.top_1_prc,
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
            set top_1_prc = $2
                , top_1k = $3
                , top_100 = $4
                , top_10 = $5
            where height = $1;"),
            &[
                &height,
                &rec.top_1_prc,
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
            limit greatest(1000::bigint, $1)
        )
        select coalesce((select sum(r.value) from ranked_addresses r where value_rank <= $1), 0)::bigint as p1
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
            limit greatest(1000::bigint, $1)
        )
        select coalesce((select sum(r.value) from ranked_addresses r where value_rank <= $1), 0)::bigint as p1
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
            limit greatest(1000::bigint, $1)
        )
        select coalesce((select sum(r.value) from ranked_addresses r where value_rank <= $1), 0)::bigint as p1
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 1000), 0)::bigint as t1k
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 100), 0)::bigint as t100
            , coalesce((select sum(value) from ranked_addresses where value_rank <= 10), 0)::bigint as t10;
        ";
}

mod sql_bs {
    pub(super) const GENERATE_TOP_BALANCES_P2PK: &str = 
        "insert into mtr._top_address_balances_p2pk (address_id, value)
            select b.address_id
                , b.value
            from mtr_sd_adr.erg b
            join core.addresses a on a.id = b.address_id
            left join cex.addresses c on c.address_id = b.address_id and c.type = 'main'
            where a.p2pk
                -- ignore main cex addresses
                and c.address_id is null
            order by value desc
            limit $1;
        ";

    pub(super) const GENERATE_TOP_BALANCES_CONTRACTS: &str = 
        "
        insert into mtr._top_address_balances_contracts (address_id, value)
            select b.address_id
                , b.value
            from mtr_sd_adr.erg b
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
            limit $1;
        ";

    pub(super) const GENERATE_TOP_BALANCES_MINERS: &str = 
        "
        insert into mtr._top_address_balances_miners (address_id, value)
            select b.address_id
                , b.value
            from mtr_sd_adr.erg b
            join core.addresses a on a.id = b.address_id
            where a.miner
            order by value desc
            limit $1;
        ";

    pub(super) const UPDATE_TOP_BALANCES_P2PK: &str = "
        insert into mtr._top_address_balances_p2pk (address_id, value)
            with changed_addresses as (
                select d.address_id
                    , sum(value)
                from adr.erg_diffs d
                join core.addresses a on a.id = d.address_id
                left join cex.addresses c on c.address_id = d.address_id and c.type = 'main'
                where d.height = $1
                    and a.p2pk
                    -- ignore main cex addresses
                    and c.address_id is null
                group by 1 having sum(value) <> 0
            )
            select d.address_id
                , b.value as balance
            from changed_addresses d
            left join mtr_sd_adr.erg b on b.address_id = d.address_id
            left join mtr._top_address_balances_p2pk t
                on t.address_id = d.address_id
            where t.address_id is not null
                or b.value > (select min(value) from mtr._top_address_balances_p2pk)
            on conflict (address_id) do update set value = EXCLUDED.value;
        ";

    pub(super) const UPDATE_TOP_BALANCES_CONTRACTS: &str = "
        insert into mtr._top_address_balances_contracts (address_id, value)
            with changed_addresses as (
                select d.address_id
                    , sum(value)
                from adr.erg_diffs d
                join core.addresses a on a.id = d.address_id
                where d.height = $1
                    and not a.p2pk
                    and not a.miner
                    -- exclude emission contracts
                    and d.address_id <> core.address_id('2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU')
                    and d.address_id <> coalesce(core.address_id('22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77'), 0)
                    and d.address_id <> coalesce(core.address_id('6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p'), 0)
                    -- exclude treasury contract
                    and d.address_id <> coalesce(core.address_id('4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy'), 0)
                group by 1 having sum(value) <> 0
            )
            select d.address_id
                , b.value as balance
            from changed_addresses d
            left join mtr_sd_adr.erg b on b.address_id = d.address_id
            left join mtr._top_address_balances_contracts t
                on t.address_id = d.address_id
            where t.address_id is not null
                or b.value > (select min(value) from mtr._top_address_balances_contracts)
            on conflict (address_id) do update set value = EXCLUDED.value;
        ";

        pub(super) const UPDATE_TOP_BALANCES_MINERS: &str = "
        insert into mtr._top_address_balances_miners (address_id, value)
            with changed_addresses as (
                select d.address_id
                    , sum(value)
                from adr.erg_diffs d
                join core.addresses a on a.id = d.address_id
                where d.height = $1
                    and a.miner
                group by 1 having sum(value) <> 0
            )
            select d.address_id
                , b.value as balance
            from changed_addresses d
            left join mtr_sd_adr.erg b on b.address_id = d.address_id
            left join mtr._top_address_balances_miners t
                on t.address_id = d.address_id
            where t.address_id is not null
                or b.value > (select min(value) from mtr._top_address_balances_miners)
            on conflict (address_id) do update set value = EXCLUDED.value;
        ";

    pub(super) const PRUNE_TOP_BALANCES_P2PK: &str = "delete from mtr._top_address_balances_p2pk where value is null;";
    pub(super) const PRUNE_TOP_BALANCES_CONTRACTS: &str = "delete from mtr._top_address_balances_contracts where value is null;";
    pub(super) const PRUNE_TOP_BALANCES_MINERS: &str = "delete from mtr._top_address_balances_miners where value is null;";
    
    pub(super) const GET_SIZE_P2PK: &str = "select count(*) from mtr._top_address_balances_p2pk";
    pub(super) const GET_SIZE_CONTRACTS: &str = "select count(*) from mtr._top_address_balances_contracts";
    pub(super) const GET_SIZE_MINERS: &str = "select count(*) from mtr._top_address_balances_miners";

    pub(super) const TRIM_TOP_BALANCES_P2PK: &str = "
        delete from mtr._top_address_balances_p2pk
        where value < (
            select value
            from mtr._top_address_balances_p2pk
            order by value
            limit 1 offset $1
        );";

    pub(super) const TRIM_TOP_BALANCES_CONTRACTS: &str = "
        delete from mtr._top_address_balances_contracts
        where value < (
            select value
            from mtr._top_address_balances_contracts
            order by value
            limit 1 offset $1
        );";
    
    pub(super) const TRIM_TOP_BALANCES_MINERS: &str = "
        delete from mtr._top_address_balances_miners
        where value < (
            select value
            from mtr._top_address_balances_miners
            order by value
            limit 1 offset $1
        );";

    pub(super) const GET_SNAPSHOT_P2PK: &str = 
        "
        select coalesce((select sum(value) from (select value from mtr._top_address_balances_p2pk order by value desc limit $1) sq), 0)::bigint as p1
            , coalesce((select sum(value) from (select value from mtr._top_address_balances_p2pk order by value desc limit 1000) sq), 0)::bigint as t1k
            , coalesce((select sum(value) from (select value from mtr._top_address_balances_p2pk order by value desc limit 100) sq), 0)::bigint as t100
            , coalesce((select sum(value) from (select value from mtr._top_address_balances_p2pk order by value desc limit 10) sq), 0)::bigint as t10;
        ";

    pub(super) const GET_SNAPSHOT_CONTRACTS: &str = 
        "
        select coalesce((select sum(value) from (select value from mtr._top_address_balances_contracts order by value desc limit $1) sq), 0)::bigint as p1
            , coalesce((select sum(value) from (select value from mtr._top_address_balances_contracts order by value desc limit 1000) sq), 0)::bigint as t1k
            , coalesce((select sum(value) from (select value from mtr._top_address_balances_contracts order by value desc limit 100) sq), 0)::bigint as t100
            , coalesce((select sum(value) from (select value from mtr._top_address_balances_contracts order by value desc limit 10) sq), 0)::bigint as t10;
        ";
    
    pub(super) const GET_SNAPSHOT_MINERS: &str = 
        "
        select coalesce((select sum(value) from (select value from mtr._top_address_balances_miners order by value desc limit $1) sq), 0)::bigint as p1
            , coalesce((select sum(value) from (select value from mtr._top_address_balances_miners order by value desc limit 1000) sq), 0)::bigint as t1k
            , coalesce((select sum(value) from (select value from mtr._top_address_balances_miners order by value desc limit 100) sq), 0)::bigint as t100
            , coalesce((select sum(value) from (select value from mtr._top_address_balances_miners order by value desc limit 10) sq), 0)::bigint as t10;
        ";
}	

#[cfg(test)]
mod tests {
    use super::first_percentile_rank;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_1st_percentile_rank() -> () {
        // Min value should be 1
        assert_eq!(first_percentile_rank(34), 1);
        // Ranks are rounded to closest int
        assert_eq!(first_percentile_rank(1490), 15);
        assert_eq!(first_percentile_rank(1510), 15);
    }
}

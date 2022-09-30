/// Adcress counts by balance
use crate::db::addresses;
use crate::parsing::BlockData;
use log::info;
use postgres::types::Type;
use postgres::Client;
use postgres::Row;
use postgres::Transaction;
use std::time::Instant;

// core.addresses p2pk flag values
const P2PK: bool = true;
const CONTRACTS: bool = false;
const NOT_MINER: bool = false;
const MINER: bool = true;

pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    // Get changes in address counts by balance
    let p2pk_diffs = get_count_diffs(tx, block.height, P2PK, NOT_MINER);
    let contract_diffs = get_count_diffs(tx, block.height, CONTRACTS, NOT_MINER);
    let miner_diffs = get_count_diffs(tx, block.height, CONTRACTS, MINER);

    // New snapshots = previous snapshot + diffs
    let p2pk_counts = cache.p2pk_counts + p2pk_diffs;
    let contract_counts = cache.contract_counts + contract_diffs;
    let miner_counts = cache.miner_counts + miner_diffs;

    // Update cache
    cache.p2pk_counts = p2pk_counts;
    cache.contract_counts = contract_counts;
    cache.miner_counts = miner_counts;

    // Insert new snapshots
    insert_p2pk_record(tx, block.height, p2pk_counts);
    insert_contract_record(tx, block.height, contract_counts);
    insert_miner_record(tx, block.height, contract_counts);
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    // Get changes in address counts by balance
    let p2pk_diffs = get_count_diffs(tx, block.height, P2PK, NOT_MINER);
    let contract_diffs = get_count_diffs(tx, block.height, CONTRACTS, NOT_MINER);
    let miner_diffs = get_count_diffs(tx, block.height, CONTRACTS, MINER);

    // Update cache
    cache.p2pk_counts -= p2pk_diffs;
    cache.contract_counts -= contract_diffs;
    cache.miner_counts -= miner_diffs;

    // Delete snapshots
    tx.execute(
        "delete from mtr.address_counts_by_balance_p2pk where height = $1",
        &[&block.height],
    )
    .unwrap();
    tx.execute(
        "delete from mtr.address_counts_by_balance_contracts where height = $1",
        &[&block.height],
    )
    .unwrap();
    tx.execute(
        "delete from mtr.address_counts_by_balance_miners where height = $1",
        &[&block.height],
    )
    .unwrap();
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
    info!("Bootstrapping metrics - address counts");

    let replay_id = "mtr_ac";

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
    tx.execute(&format!("set local work_mem = {};", work_mem_kb), &[])?;
    addresses::replay::prepare(&mut tx, sync_height, replay_id);
    tx.commit()?;

    // Init counts
    let mut p2pk_counts: Record = client
        .query_one(
            &GET_SNAPSHOT.replace(" adr.erg ", &format!(" {replay_id}_adr.erg ")),
            &[&P2PK, &NOT_MINER],
        )?
        .into();
    let mut cons_counts: Record = client
        .query_one(
            &GET_SNAPSHOT.replace(" adr.erg ", &format!(" {replay_id}_adr.erg ")),
            &[&CONTRACTS, &NOT_MINER],
        )?
        .into();
    let mut mins_counts: Record = client
        .query_one(
            &GET_SNAPSHOT.replace(" adr.erg ", &format!(" {replay_id}_adr.erg ")),
            &[&CONTRACTS, &MINER],
        )?
        .into();

    // Bootstrapping will be performed in batches of 1000
    let batch_size = 1000;
    let batches = blocks.chunks(batch_size);
    let nb_batches = batches.len();

    for (ibatch, batch_blocks) in batches.enumerate() {
        let timer = Instant::now();
        let mut tx = client.transaction()?;

        tx.execute(&format!("set local work_mem = {};", work_mem_kb), &[])?;

        // Prepare statements
        let stmt_get_diffs = tx.prepare_typed(
            &GET_DIFFS_AT_HEIGHT.replace(" adr.erg ", &format!(" {replay_id}_adr.erg ")),
            &[Type::INT4, Type::BOOL],
        )?;

        for height in batch_blocks {
            // step replay
            addresses::replay::step(&mut tx, *height, replay_id);

            // get diffs
            let p2pk_diffs: Record = tx
                .query_one(&stmt_get_diffs, &[&height, &P2PK, &NOT_MINER])?
                .into();
            let cons_diffs: Record = tx
                .query_one(&stmt_get_diffs, &[&height, &CONTRACTS, &NOT_MINER])?
                .into();
            let mins_diffs: Record = tx
                .query_one(&stmt_get_diffs, &[&height, &CONTRACTS, &MINER])?
                .into();

            // update cache
            p2pk_counts += p2pk_diffs;
            cons_counts += cons_diffs;
            mins_counts += mins_diffs;

            // insert records
            insert_p2pk_record(&mut tx, *height, p2pk_counts);
            insert_contract_record(&mut tx, *height, cons_counts);
            insert_miner_record(&mut tx, *height, mins_counts);
        }

        tx.commit()?;

        info!(
            "Bootstrapping address count metrics - batch {} / {} (processed in {}s)",
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
        .query_one("select address_counts_bootstrapped from mtr._log;", &[])
        .unwrap();
    row.get(0)
}

fn constraints_are_set(client: &mut Client) -> bool {
    let row = client
        .query_one("select address_counts_constraints_set from mtr._log;", &[])
        .unwrap();
    row.get(0)
}

/// Get sync height of address counts tables.
fn get_sync_height(client: &mut Client) -> Option<i32> {
    // P2PK and other tables are progressed in sync, so enough to probe only one.
    let row = client
        .query_one(
            "select max(height) from mtr.address_counts_by_balance_p2pk;",
            &[],
        )
        .unwrap();
    row.get(0)
}

fn set_constraints(client: &mut Client) {
    let statements = vec![
        // P2PK
        "alter table mtr.address_counts_by_balance_p2pk add primary key(height);",
        "alter table mtr.address_counts_by_balance_p2pk alter column height set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column total set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_0p001 set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_0p01 set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_0p1 set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_1 set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_10 set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_100 set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_1k set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_10k set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_100k set not null;",
        "alter table mtr.address_counts_by_balance_p2pk alter column ge_1m set not null;",
        // Contracts
        "alter table mtr.address_counts_by_balance_contracts add primary key(height);",
        "alter table mtr.address_counts_by_balance_contracts alter column height set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column total set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_0p001 set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_0p01 set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_0p1 set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_1 set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_10 set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_100 set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_1k set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_10k set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_100k set not null;",
        "alter table mtr.address_counts_by_balance_contracts alter column ge_1m set not null;",
        // Miners
        "alter table mtr.address_counts_by_balance_miners add primary key(height);",
        "alter table mtr.address_counts_by_balance_miners alter column height set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column total set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_0p001 set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_0p01 set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_0p1 set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_1 set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_10 set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_100 set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_1k set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_10k set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_100k set not null;",
        "alter table mtr.address_counts_by_balance_miners alter column ge_1m set not null;",
        // Flag
        "update mtr._log set address_counts_constraints_set = TRUE;",
    ];
    let mut tx = client.transaction().unwrap();
    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
    tx.commit().unwrap();
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Record {
    pub total: i64,
    pub ge_0p001: i64,
    pub ge_0p01: i64,
    pub ge_0p1: i64,
    pub ge_1: i64,
    pub ge_10: i64,
    pub ge_100: i64,
    pub ge_1k: i64,
    pub ge_10k: i64,
    pub ge_100k: i64,
    pub ge_1m: i64,
}

impl Record {
    fn new() -> Self {
        Self {
            total: 0,
            ge_0p001: 0,
            ge_0p01: 0,
            ge_0p1: 0,
            ge_1: 0,
            ge_10: 0,
            ge_100: 0,
            ge_1k: 0,
            ge_10k: 0,
            ge_100k: 0,
            ge_1m: 0,
        }
    }
}

impl std::ops::Add for Record {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            total: self.total + other.total,
            ge_0p001: self.ge_0p001 + other.ge_0p001,
            ge_0p01: self.ge_0p01 + other.ge_0p01,
            ge_0p1: self.ge_0p1 + other.ge_0p1,
            ge_1: self.ge_1 + other.ge_1,
            ge_10: self.ge_10 + other.ge_10,
            ge_100: self.ge_100 + other.ge_100,
            ge_1k: self.ge_1k + other.ge_1k,
            ge_10k: self.ge_10k + other.ge_10k,
            ge_100k: self.ge_100k + other.ge_100k,
            ge_1m: self.ge_1m + other.ge_1m,
        }
    }
}

impl std::ops::AddAssign for Record {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            total: self.total + other.total,
            ge_0p001: self.ge_0p001 + other.ge_0p001,
            ge_0p01: self.ge_0p01 + other.ge_0p01,
            ge_0p1: self.ge_0p1 + other.ge_0p1,
            ge_1: self.ge_1 + other.ge_1,
            ge_10: self.ge_10 + other.ge_10,
            ge_100: self.ge_100 + other.ge_100,
            ge_1k: self.ge_1k + other.ge_1k,
            ge_10k: self.ge_10k + other.ge_10k,
            ge_100k: self.ge_100k + other.ge_100k,
            ge_1m: self.ge_1m + other.ge_1m,
        };
    }
}

impl std::ops::SubAssign for Record {
    fn sub_assign(&mut self, other: Self) {
        *self = Self {
            total: self.total - other.total,
            ge_0p001: self.ge_0p001 - other.ge_0p001,
            ge_0p01: self.ge_0p01 - other.ge_0p01,
            ge_0p1: self.ge_0p1 - other.ge_0p1,
            ge_1: self.ge_1 - other.ge_1,
            ge_10: self.ge_10 - other.ge_10,
            ge_100: self.ge_100 - other.ge_100,
            ge_1k: self.ge_1k - other.ge_1k,
            ge_10k: self.ge_10k - other.ge_10k,
            ge_100k: self.ge_100k - other.ge_100k,
            ge_1m: self.ge_1m - other.ge_1m,
        };
    }
}

impl From<Row> for Record {
    fn from(row: Row) -> Record {
        Record {
            total: row.get(0),
            ge_0p001: row.get(1),
            ge_0p01: row.get(2),
            ge_0p1: row.get(3),
            ge_1: row.get(4),
            ge_10: row.get(5),
            ge_100: row.get(6),
            ge_1k: row.get(7),
            ge_10k: row.get(8),
            ge_100k: row.get(9),
            ge_1m: row.get(10),
        }
    }
}

/// Generate an address count snapshot from scratch
fn get_count_snapshot(tx: &mut Transaction, p2pk: bool, miner: bool) -> Record {
    tx.query_one(GET_SNAPSHOT, &[&p2pk, &miner]).unwrap().into()
}

/// Get address count difference at given height
fn get_count_diffs(tx: &mut Transaction, height: i32, p2pk: bool, miner: bool) -> Record {
    tx.query_one(GET_DIFFS_AT_HEIGHT, &[&height, &p2pk, &miner])
        .unwrap()
        .into()
}

const GET_DIFFS_AT_HEIGHT: &str = "
    with diffs as (
        select d.address_id
            , sum(d.value) as value
        from adr.erg_diffs d
        join core.addresses a on a.id = d.address_id
        where d.height = $1
            and a.p2pk = $2
            and a.miner = $3
            and d.address_id <> core.address_id('2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU')
            and d.address_id <> coalesce(core.address_id('22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77'), 0)
            and d.address_id <> coalesce(core.address_id('6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p'), 0)
        group by 1
    )
    select count(*) filter (where b.value > 0) - count(*) filter (where coalesce(b.value, 0) - d.value > 0) as ge_0
        , count(*) filter (where b.value >= 10^6) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^6) as ge_0p001
        , count(*) filter (where b.value >= 10^7) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^7) as ge_0p01
        , count(*) filter (where b.value >= 10^8) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^8) as ge_0p1
        , count(*) filter (where b.value >= 10^9) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^9) as ge_1
        , count(*) filter (where b.value >= 10^10) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^10) as ge_10
        , count(*) filter (where b.value >= 10^11) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^11) as ge_100
        , count(*) filter (where b.value >= 10^12) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^12) as ge_1k
        , count(*) filter (where b.value >= 10^13) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^13) as ge_10k
        , count(*) filter (where b.value >= 10^14) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^14) as ge_100k
        , count(*) filter (where b.value >= 10^15) - count(*) filter (where coalesce(b.value, 0) - d.value >= 10^15) as ge_1m
    from diffs d
    left join adr.erg b on b.address_id = d.address_id
    ";

const GET_SNAPSHOT: &str = "
    select count(*) as all
        , count(*) filter (where b.value >= 10^6) as ge_0p001
        , count(*) filter (where b.value >= 10^7) as ge_0p01
        , count(*) filter (where b.value >= 10^8) as ge_0p1
        , count(*) filter (where b.value >= 10^9) as ge_1
        , count(*) filter (where b.value >= 10^10) as ge_10
        , count(*) filter (where b.value >= 10^11) as ge_100
        , count(*) filter (where b.value >= 10^12) as ge_1k
        , count(*) filter (where b.value >= 10^13) as ge_10k
        , count(*) filter (where b.value >= 10^14) as ge_100k
        , count(*) filter (where b.value >= 10^15) as ge_1m        
    from adr.erg b
    join core.addresses a on a.id = b.address_id
    where a.p2pk = $1
        and a.miner = $2
        and b.address_id <> core.address_id('2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU')
        and b.address_id <> coalesce(core.address_id('22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77'), 0)
        and b.address_id <> coalesce(core.address_id('6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p'), 0)
    ;";

fn insert_p2pk_record(tx: &mut Transaction, height: i32, rec: Record) {
    let table = "mtr.address_counts_by_balance_p2pk";
    insert_record(tx, height, rec, table);
}

fn insert_contract_record(tx: &mut Transaction, height: i32, rec: Record) {
    let table = "mtr.address_counts_by_balance_contracts";
    insert_record(tx, height, rec, table);
}

fn insert_miner_record(tx: &mut Transaction, height: i32, rec: Record) {
    let table = "mtr.address_counts_by_balance_miners";
    insert_record(tx, height, rec, table);
}

fn insert_record(tx: &mut Transaction, height: i32, rec: Record, table: &str) {
    tx.execute(
        &format!(
            "
            insert into {table} (
                height,
                total,
                ge_0p001,
                ge_0p01,
                ge_0p1,
                ge_1,
                ge_10,
                ge_100,
                ge_1k,
                ge_10k,
                ge_100k,
                ge_1m
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
        "
        ),
        &[
            &height,
            &rec.total,
            &rec.ge_0p001,
            &rec.ge_0p01,
            &rec.ge_0p1,
            &rec.ge_1,
            &rec.ge_10,
            &rec.ge_100,
            &rec.ge_1k,
            &rec.ge_10k,
            &rec.ge_100k,
            &rec.ge_1m,
        ],
    )
    .unwrap();
}

#[derive(Debug)]
pub struct Cache {
    pub p2pk_counts: Record,
    pub contract_counts: Record,
    pub miner_counts: Record,
}

impl Cache {
    pub(super) fn new() -> Self {
        Self {
            p2pk_counts: Record::new(),
            contract_counts: Record::new(),
            miner_counts: Record::new(),
        }
    }

    pub(super) fn load(client: &mut Client) -> Self {
        let mut tx = client.transaction().unwrap();
        Self {
            p2pk_counts: get_count_snapshot(&mut tx, P2PK, NOT_MINER),
            contract_counts: get_count_snapshot(&mut tx, CONTRACTS, NOT_MINER),
            miner_counts: get_count_snapshot(&mut tx, CONTRACTS, MINER),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Record;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_record_add() -> () {
        let a = Record {
            total: 10,
            ge_0p001: 20,
            ge_0p01: 30,
            ge_0p1: 40,
            ge_1: 50,
            ge_10: 60,
            ge_100: 70,
            ge_1k: 80,
            ge_10k: 90,
            ge_100k: 100,
            ge_1m: 110,
        };
        let b = Record {
            total: 1,
            ge_0p001: 2,
            ge_0p01: 3,
            ge_0p1: 4,
            ge_1: 5,
            ge_10: 6,
            ge_100: 7,
            ge_1k: 8,
            ge_10k: 9,
            ge_100k: 10,
            ge_1m: 11,
        };
        let expected = Record {
            total: 11,
            ge_0p001: 22,
            ge_0p01: 33,
            ge_0p1: 44,
            ge_1: 55,
            ge_10: 66,
            ge_100: 77,
            ge_1k: 88,
            ge_10k: 99,
            ge_100k: 110,
            ge_1m: 121,
        };
        assert_eq!(a + b, expected);
    }

    #[test]
    fn test_record_addassign() -> () {
        let mut a = Record {
            total: 10,
            ge_0p001: 20,
            ge_0p01: 30,
            ge_0p1: 40,
            ge_1: 50,
            ge_10: 60,
            ge_100: 70,
            ge_1k: 80,
            ge_10k: 90,
            ge_100k: 100,
            ge_1m: 110,
        };
        let b = Record {
            total: 1,
            ge_0p001: 2,
            ge_0p01: 3,
            ge_0p1: 4,
            ge_1: 5,
            ge_10: 6,
            ge_100: 7,
            ge_1k: 8,
            ge_10k: 9,
            ge_100k: 10,
            ge_1m: 11,
        };
        let expected = Record {
            total: 11,
            ge_0p001: 22,
            ge_0p01: 33,
            ge_0p1: 44,
            ge_1: 55,
            ge_10: 66,
            ge_100: 77,
            ge_1k: 88,
            ge_10k: 99,
            ge_100k: 110,
            ge_1m: 121,
        };
        a += b;
        assert_eq!(a, expected);
    }

    #[test]
    fn test_record_subassign() -> () {
        let mut a = Record {
            total: 10,
            ge_0p001: 20,
            ge_0p01: 30,
            ge_0p1: 40,
            ge_1: 50,
            ge_10: 60,
            ge_100: 70,
            ge_1k: 80,
            ge_10k: 90,
            ge_100k: 100,
            ge_1m: 110,
        };
        let b = Record {
            total: 1,
            ge_0p001: 2,
            ge_0p01: 3,
            ge_0p1: 4,
            ge_1: 5,
            ge_10: 6,
            ge_100: 7,
            ge_1k: 8,
            ge_10k: 9,
            ge_100k: 10,
            ge_1m: 11,
        };
        let expected = Record {
            total: 10 - 1,
            ge_0p001: 20 - 2,
            ge_0p01: 30 - 3,
            ge_0p1: 40 - 4,
            ge_1: 50 - 5,
            ge_10: 60 - 6,
            ge_100: 70 - 7,
            ge_1k: 80 - 8,
            ge_10k: 90 - 9,
            ge_100k: 100 - 10,
            ge_1m: 110 - 11,
        };
        a -= b;
        assert_eq!(a, expected);
    }
}

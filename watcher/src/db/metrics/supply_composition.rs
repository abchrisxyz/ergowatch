/// Supply composition
///
/// Emitted supply on various address types.
use crate::emission;
use crate::parsing::BlockData;
use log::info;
use postgres::types::Type;
use postgres::Client;
use postgres::Row;
use postgres::Transaction;
use std::time::Instant;

pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    // Get changes in address counts by balance
    let diffs = get_diffs(tx, block.height);

    // New snapshots = previous snapshot + diffs
    cache.prev = cache.curr;
    cache.curr.height = block.height;
    cache.curr.composition += diffs;

    // Insert new record
    insert_record(tx, &cache.curr);
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    // Delete snapshot
    tx.execute(sql::DELETE_RECORD_AT, &[&block.height]).unwrap();

    // Update cache
    cache.curr = cache.prev;
    cache.prev = match tx.query_opt(sql::GET_PREVIOUS_RECORD, &[]).unwrap() {
        Some(row) => row.into(),
        None => Record::new(),
    };
    assert_eq!(cache.curr.height, block.height - 1);
    assert_eq!(cache.prev.height, cache.curr.height - 1);
}

pub(super) fn repair(tx: &mut Transaction, height: i32) {
    // Get changes in address counts by balance
    let mut diffs: Composition = get_diffs(tx, height);

    // Don't exclude the possibility of repairs going back to
    // treasury rewards era and append possible reward to diff.
    diffs.treasury += emission::treasury_reward_at_height(height as i64);

    // Get previous record
    let prev: Record = tx
        .query_one(sql::GET_RECORD_AT, &[&(height - 1)])
        .unwrap()
        .into();

    // Update record with new values
    update_record(tx, height, &(prev.composition + diffs));
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
    info!("Bootstrapping metrics - supply composition");

    // Retrieve heights to process
    let sync_height = match get_sync_height(client) {
        Some(h) => h,
        None => -1,
    };
    let mut blocks: Vec<i32> = client
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

    // Init cache
    // The prev record is not needed during bootstrapping, so use a single record as cache,
    // not the actual Cache type.
    let mut cache: Record = Cache::load(client).curr;

    // Handle genesis block differently because treasury deposit
    // at height 0 is still entirely locked.
    if blocks[0] == 0 {
        let height = 0i32;
        let mut tx = client.transaction().unwrap();
        let mut diffs: Composition = tx.query_one(sql::GET_DIFFS_AT, &[&height]).unwrap().into();

        // Overwrite treasury diff as still entirely locked
        diffs.treasury = 0;

        // Update cache
        cache.height = height;
        cache.composition += diffs;

        // Insert record
        insert_record(&mut tx, &cache);

        tx.commit().unwrap();
        blocks.remove(0);
    }

    // Bootstrapping will be performed in batches of 1000
    let batch_size = 1000;
    let batches = blocks.chunks(batch_size);
    let nb_batches = batches.len();

    for (ibatch, batch_blocks) in batches.enumerate() {
        let timer = Instant::now();
        let mut tx = client.transaction()?;

        tx.execute(&format!("set local work_mem = {};", work_mem_kb), &[])?;

        // Prepare statements
        let diffs_stmt = tx.prepare_typed(sql::GET_DIFFS_AT, &[Type::INT4])?;

        for height in batch_blocks {
            // Get diffs
            let mut diffs: Composition = tx.query_one(&diffs_stmt, &[height]).unwrap().into();

            // Treasury funds are unlocked, not transferred, so they won't show up in balance diffs.
            // Adding them here instead.
            diffs.treasury += emission::treasury_reward_at_height(*height as i64);

            // Update cache
            cache.height = *height;
            cache.composition += diffs;

            // Insert record
            insert_record(&mut tx, &cache);
        }

        tx.commit()?;

        info!(
            "Bootstrapping supply composition - batch {} / {} (processed in {:.2}s)",
            ibatch + 1,
            nb_batches,
            timer.elapsed().as_secs_f32()
        );
    }

    client.execute(
        "update mtr._log set supply_composition_bootstrapped = TRUE;",
        &[],
    )?;

    Ok(())
}

fn is_bootstrapped(client: &mut Client) -> bool {
    let row = client
        .query_one("select supply_composition_bootstrapped from mtr._log;", &[])
        .unwrap();
    row.get(0)
}

fn constraints_are_set(client: &mut Client) -> bool {
    let row = client
        .query_one(
            "select supply_composition_constraints_set from mtr._log;",
            &[],
        )
        .unwrap();
    row.get(0)
}

fn get_sync_height(client: &mut Client) -> Option<i32> {
    let row = client
        .query_one("select max(height) from mtr.supply_composition;", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(client: &mut Client) {
    let statements = vec![
        "alter table mtr.supply_composition add primary key (height);",
        "alter table mtr.supply_composition alter column p2pks set not null;",
        "alter table mtr.supply_composition alter column cex_main set not null;",
        "alter table mtr.supply_composition alter column cex_deposits set not null;",
        "alter table mtr.supply_composition alter column contracts set not null;",
        "alter table mtr.supply_composition alter column miners set not null;",
        "alter table mtr.supply_composition alter column treasury set not null;",
        // Flag
        "update mtr._log set supply_composition_constraints_set = TRUE;",
    ];
    let mut tx = client.transaction().unwrap();
    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
    tx.commit().unwrap();
}

fn insert_record(tx: &mut Transaction, rec: &Record) {
    tx.execute(
        "
        insert into mtr.supply_composition (
            height,
            p2pks,
            cex_main,
            cex_deposits,
            contracts,
            miners,
            treasury
        ) values ($1, $2, $3, $4, $5, $6, $7);",
        &[
            &rec.height,
            &rec.composition.p2pks,
            &rec.composition.cex_main,
            &rec.composition.cex_deposits,
            &rec.composition.contracts,
            &rec.composition.miners,
            &rec.composition.treasury,
        ],
    )
    .unwrap();
}

fn update_record(tx: &mut Transaction, height: i32, composition: &Composition) {
    tx.execute(
        &format!(
            "
            update mtr.supply_composition
            set  p2pks = $2
                , cex_main = $3
                , cex_deposits = $4
                , contracts = $5
                , miners = $6
                , treasury = $7
            where height = $1;"
        ),
        &[
            &height,
            &composition.p2pks,
            &composition.cex_main,
            &composition.cex_deposits,
            &composition.contracts,
            &composition.miners,
            &composition.treasury,
        ],
    )
    .unwrap();
}

#[derive(Debug, Clone, Copy)]
pub struct Record {
    pub height: i32,
    pub composition: Composition,
}

impl Record {
    pub fn new() -> Self {
        Self {
            height: 0,
            composition: Composition::new(),
        }
    }

    /// Get records from `h_ge` to `h_le` included
    ///
    /// `h_first`: height of first record
    /// `h_last`: height of last record (>= h_first)
    pub fn get_range(client: &mut Transaction, h_first: i32, h_last: i32) -> Vec<Self> {
        let rows = client
            .query(sql::GET_RECORD_RANGE, &[&h_first, &h_last])
            .unwrap();
        rows.iter().map(|r| Record::from(r)).collect()
    }
}

impl From<Row> for Record {
    fn from(row: Row) -> Self {
        Self {
            height: row.get(0),
            composition: Composition {
                p2pks: row.get(1),
                cex_main: row.get(2),
                cex_deposits: row.get(3),
                contracts: row.get(4),
                miners: row.get(5),
                treasury: row.get(6),
            },
        }
    }
}

impl From<&Row> for Record {
    fn from(row: &Row) -> Self {
        Self {
            height: row.get(0),
            composition: Composition {
                p2pks: row.get(1),
                cex_main: row.get(2),
                cex_deposits: row.get(3),
                contracts: row.get(4),
                miners: row.get(5),
                treasury: row.get(6),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Composition {
    pub p2pks: i64,
    pub cex_main: i64,
    pub cex_deposits: i64,
    pub contracts: i64,
    pub miners: i64,
    pub treasury: i64,
}

impl Composition {
    pub fn new() -> Self {
        Self {
            p2pks: 0,
            cex_main: 0,
            cex_deposits: 0,
            contracts: 0,
            miners: 0,
            treasury: 0,
        }
    }
}

impl std::ops::Add for Composition {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            p2pks: self.p2pks + other.p2pks,
            cex_main: self.cex_main + other.cex_main,
            cex_deposits: self.cex_deposits + other.cex_deposits,
            contracts: self.contracts + other.contracts,
            miners: self.miners + other.miners,
            treasury: self.treasury + other.treasury,
        }
    }
}

impl std::ops::AddAssign for Composition {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            p2pks: self.p2pks + other.p2pks,
            cex_main: self.cex_main + other.cex_main,
            cex_deposits: self.cex_deposits + other.cex_deposits,
            contracts: self.contracts + other.contracts,
            miners: self.miners + other.miners,
            treasury: self.treasury + other.treasury,
        };
    }
}

impl From<Row> for Composition {
    fn from(row: Row) -> Self {
        Self {
            p2pks: row.get(0),
            cex_main: row.get(1),
            cex_deposits: row.get(2),
            contracts: row.get(3),
            miners: row.get(4),
            treasury: row.get(5),
        }
    }
}

fn get_diffs(tx: &mut Transaction, height: i32) -> Composition {
    tx.query_one(sql::GET_DIFFS_AT, &[&height]).unwrap().into()
}

#[derive(Debug)]
pub struct Cache {
    pub curr: Record,
    pub prev: Record,
}

impl Cache {
    pub(super) fn new() -> Self {
        Self {
            curr: Record::new(),
            prev: Record::new(),
        }
    }

    pub(super) fn load(client: &mut Client) -> Self {
        Self {
            curr: match client.query_opt(sql::GET_LATEST_RECORD, &[]).unwrap() {
                Some(row) => row.into(),
                None => Record::new(),
            },
            prev: match client.query_opt(sql::GET_PREVIOUS_RECORD, &[]).unwrap() {
                Some(row) => row.into(),
                None => Record::new(),
            },
        }
    }
}

mod sql {
    /// Supply diffs by category
    ///
    /// $1: height
    pub(super) const GET_DIFFS_AT: &str = "
        select coalesce(sum(value) filter (where a.p2pk and c.address_id is null), 0)::bigint as d_p2pk
            , coalesce(sum(value) filter (where a.p2pk and c.type = 'main'), 0)::bigint as d_cex_m
            , coalesce(sum(value) filter (where a.p2pk and c.type = 'deposit'), 0)::bigint as d_cex_d
            , coalesce(sum(value) filter (where not a.p2pk and not a.miner and a.id <> core.address_id('4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy')), 0)::bigint as d_cons
            , coalesce(sum(value) filter (where a.miner), 0)::bigint as d_miner
            , coalesce(sum(value) filter (where a.id = core.address_id('4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy')), 0)::bigint as d_tres
        from adr.erg_diffs d
        join core.addresses a on a.id = d.address_id
        left join cex.addresses c on c.address_id = d.address_id
        where d.height = $1
            -- exclude emission contracts
            and d.address_id <> core.address_id('2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU')
            and d.address_id <> coalesce(core.address_id('22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77'), 0)
            and d.address_id <> coalesce(core.address_id('6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p'), 0)
            -- exclude fee contract as always net zero
            and d.address_id <> coalesce(core.address_id('2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe'), 0)
            ;
    ";

    pub(super) const DELETE_RECORD_AT: &str =
        "delete from mtr.supply_composition where height = $1";

    pub(super) const GET_LATEST_RECORD: &str = "
        select height 
            , p2pks
            , cex_main
            , cex_deposits
            , contracts
            , miners
            , treasury
        from mtr.supply_composition
        order by height desc limit 1;
    ";

    pub(super) const GET_PREVIOUS_RECORD: &str = "
        select height
            , p2pks
            , cex_main
            , cex_deposits
            , contracts
            , miners
            , treasury
        from mtr.supply_composition
        order by height desc limit 1 offset 1;
    ";

    /// Get rows from height to height
    ///
    /// $1: first height
    /// $2: last height
    pub(super) const GET_RECORD_RANGE: &str = "
        select height
            , p2pks
            , cex_main
            , cex_deposits
            , contracts
            , miners
            , treasury
        from mtr.supply_composition
        where height >= $1 and height <= $2
        order by 1;
    ";

    pub(super) const GET_RECORD_AT: &str = "
        select height
            , p2pks
            , cex_main
            , cex_deposits
            , contracts
            , miners
            , treasury
        from mtr.supply_composition
        where height = $1;
    ";
}

#[cfg(test)]
mod tests {
    use super::Composition;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_record_add() -> () {
        let a = Composition {
            p2pks: 10,
            cex_main: 20,
            cex_deposits: 30,
            contracts: 40,
            miners: 50,
            treasury: 60,
        };
        let b = Composition {
            p2pks: 1,
            cex_main: 2,
            cex_deposits: 3,
            contracts: 4,
            miners: 5,
            treasury: 6,
        };
        let expected = Composition {
            p2pks: 11,
            cex_main: 22,
            cex_deposits: 33,
            contracts: 44,
            miners: 55,
            treasury: 66,
        };
        assert_eq!(a + b, expected);
    }

    #[test]
    fn test_record_addassign() -> () {
        let mut a = Composition {
            p2pks: 10,
            cex_main: 20,
            cex_deposits: 30,
            contracts: 40,
            miners: 50,
            treasury: 60,
        };
        let b = Composition {
            p2pks: 1,
            cex_main: 2,
            cex_deposits: 3,
            contracts: 4,
            miners: 5,
            treasury: 6,
        };
        let expected = Composition {
            p2pks: 11,
            cex_main: 22,
            cex_deposits: 33,
            contracts: 44,
            miners: 55,
            treasury: 66,
        };
        a += b;
        assert_eq!(a, expected);
    }
}

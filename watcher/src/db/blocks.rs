/// Block level stats
use super::Transaction;
use crate::emission;
use crate::parsing::BlockData;
use log::debug;
use log::info;
use postgres::types::Type;
use postgres::Client;
use std::time::Instant;

pub(super) fn include_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    let emission = emission::emission_at_height(block.height);
    let reward = emission::miner_reward_at_height(block.height);
    cache.circ_supply += emission;

    tx.execute(
        sql::INSERT_BLOCK_STATS,
        &[&block.height, &cache.circ_supply, &emission, &reward],
    )
    .unwrap();
    Ok(())
}

pub(super) fn rollback_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    let emission = emission::emission_at_height(block.height);
    cache.circ_supply -= emission;
    tx.execute(sql::DELETE_BLOCK_STATS, &[&block.height])
        .unwrap();

    Ok(())
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
    info!("Bootstrapping block stats");

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

    // Bootstrapping will be performed in batches of 10k
    let batch_size = 10000;
    let batches = blocks.chunks(batch_size);
    let nb_batches = batches.len();

    // Init cache
    let mut cache = Cache::load(client);

    for (ibatch, batch_blocks) in batches.enumerate() {
        let timer = Instant::now();
        let mut tx = client.transaction()?;

        // Prepare statements
        let stmt_insert_stats = tx.prepare_typed(
            &sql::INSERT_BLOCK_STATS,
            &[Type::INT4, Type::INT8, Type::INT8, Type::INT8],
        )?;

        for height in batch_blocks {
            let emission = emission::emission_at_height(*height);
            let reward = emission::miner_reward_at_height(*height);
            cache.circ_supply += emission;
            tx.execute(
                &stmt_insert_stats,
                &[&height, &cache.circ_supply, &emission, &reward],
            )?;
        }

        tx.commit()?;

        info!(
            "Bootstrapping block stats - batch {} / {} (processed in {}s)",
            ibatch + 1,
            nb_batches,
            timer.elapsed().as_secs()
        );
    }

    client.execute("update blk._log set bootstrapped = TRUE;", &[])?;

    Ok(())
}

fn is_bootstrapped(client: &mut Client) -> bool {
    client
        .query_one("select bootstrapped from blk._log;", &[])
        .unwrap()
        .get(0)
}

fn constraints_are_set(client: &mut Client) -> bool {
    client
        .query_one("select constraints_set from blk._log;", &[])
        .unwrap()
        .get(0)
}

fn get_sync_height(client: &mut Client) -> Option<i32> {
    let row = client
        .query_one("select max(height) from blk.stats;", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(client: &mut Client) {
    let statements = vec![
        "alter table blk.stats add primary key(height);",
        "alter table blk.stats alter column height set not null;",
        "alter table blk.stats alter column circulating_supply set not null;",
        "alter table blk.stats alter column emission set not null;",
        "alter table blk.stats alter column reward set not null;",
        "alter table blk.stats alter column tx_fees set not null;",
        "alter table blk.stats alter column tx_count set not null;",
        "alter table blk.stats alter column volume set not null;",
        // Flag
        "update blk._log set constraints_set = TRUE;",
    ];
    let mut tx = client.transaction().unwrap();
    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
    tx.commit().unwrap();
}

fn get_circulating_supply(client: &mut Client) -> i64 {
    match client
        .query_opt(
            "
            select circulating_supply
            from blk.stats
            order by height desc
            limit 1",
            &[],
        )
        .unwrap()
    {
        Some(row) => row.get(0),
        None => 0,
    }
}

mod sql {
    pub(super) const INSERT_BLOCK_STATS: &str = "
        insert into blk.stats (
            height,
            circulating_supply,
            emission,
            reward,
            tx_fees,
            tx_count,
            volume
        )
        select $1
            , $2 as circ_supply
            , $3 as emission
            , $4 as reward
            , coalesce(-sum(d.value) filter (where d.value < 0
                and d.address_id = core.address_id('2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe')						   
            ), 0) as tx_fees
            , count(distinct d.tx_id) as txs
            , coalesce(-sum(d.value) filter (where d.value < 0
                -- ignore volume out of emission contracts
                and d.address_id <> coalesce(core.address_id('2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU'), 0)
                and d.address_id <> coalesce(core.address_id('22WkKcVUvboYCZJe1urbmvBL3j67LKb5KEAvFhJXqA6ubYvHpSCvbvwvEY3xzUr7QvxpEtqjzMAPMsVdZh1VGWmZphvKoJdVzL1ayhsMftTtEFoA3YYdq3zKeeYXavVrrPUmK3fRXJ2HWEbZexewtBWcgAnHBw5tKvYFy9dEUi645gE2fYMUvVBtbvMExE9mjZ2W9goWkqu1VtThAsMZWZWjHxDjX116HpeQKu9b9neEUBj4kE5sX8QXaV6ZeReXxYHFJFg2rmaTknSPMxHXA8NpQKgzryBwLssp5EJ1QTqn5R6xuvGgFCEUZicCEo8qk8UNbE7e2d4WqW5qzpQPzJkKoPa5UtJEPYDWNhaCKmCpzdSc77'), 0)
                and d.address_id <> coalesce(core.address_id('6KxusedL87PBibr1t1f4ggzAyTAmWEPqSpqXbkdoybNwHVw5Nb7cUESBmQw5XK8TyvbQiueyqkR9XMNaUgpWx3jT54p'), 0)
                -- ignore volume out of fee contract as already included in volume out of other addresses
                and d.address_id <> coalesce(core.address_id('2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe')						   , 0)
            ), 0) as volume
        from adr.erg_diffs d
        join core.addresses a on a.id = d.address_id
        where d.height = $1;";

    pub(super) const DELETE_BLOCK_STATS: &str = "delete from blk.stats where height = $1 ;";
}

#[derive(Debug)]
pub struct Cache {
    circ_supply: i64,
}

impl Cache {
    /// Initialize a cache with default values, representing an empty database.
    pub fn new() -> Self {
        Self { circ_supply: 0i64 }
    }

    /// Load cache values from db
    pub fn load(client: &mut Client) -> Self {
        debug!("Loading blocks cache");
        Self {
            circ_supply: get_circulating_supply(client),
        }
    }
}

pub mod addresses;
pub mod blocks;
pub mod cexs;
pub mod coingecko;
pub mod core;
pub mod metrics;
mod migrations;
pub mod unspent;

use log::debug;
use log::info;
use postgres::{Client, NoTls, Transaction};

use crate::parsing::BlockData;
use crate::types::Head;

use self::cexs::deposit_addresses::AddressQueues;

#[derive(Debug)]
pub struct DB {
    conn_str: String,
    bootstrapping_work_mem_kb: u32,
    cache: Cache,
    buffer: Buffer,
}

impl DB {
    /// Add genesis boxes to database
    pub fn include_genesis_boxes(
        &self,
        boxes: Vec<crate::node::models::Output>,
    ) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut tx = client.transaction()?;
        core::genesis::include_genesis_boxes(&mut tx, &boxes);
        // Other schemas pick up genesis boxes from core tables during bootstrapping.
        tx.commit()?;
        Ok(())
    }

    /// Add block to database
    pub fn include_block(&mut self, block: &BlockData) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut tx = client.transaction()?;

        core::include_block(&mut tx, block)?;
        unspent::include_block(&mut tx, block)?;

        // Tag any unseen cex addresses here
        cexs::declaration::include_block(&mut tx, block, &mut self.cache.cexs)?;

        self.buffer.supply_age_diffs = addresses::include_block(&mut tx, block)?;
        blocks::include_block(&mut tx, block, &mut self.cache.blocks)?;
        cexs::include_block(&mut tx, block, &mut self.cache.cexs)?;
        metrics::include_block(
            &mut tx,
            block,
            &self.buffer,
            &mut self.cache.metrics,
            &self.cache.coingecko,
        )?;

        tx.commit()?;

        self.cache.last_height = block.height;

        Ok(())
    }

    /// Restore db state to what it was before including given block
    pub fn rollback_block(&mut self, block: &BlockData) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut tx = client.transaction()?;

        metrics::rollback_block(&mut tx, block, &mut self.cache.metrics)?;
        let aqs = cexs::rollback_block(&mut tx, block, &mut self.cache.cexs);
        blocks::rollback_block(&mut tx, block, &mut self.cache.blocks)?;
        addresses::rollback_block(&mut tx, block)?;
        cexs::declaration::rollback_block(&mut tx, block, &mut self.cache.cexs)?;
        unspent::rollback_block(&mut tx, block)?;
        core::rollback_block(&mut tx, block)?;

        if let Some(queues) = aqs {
            self.process_deposit_address_queues(&mut tx, queues);
        }

        tx.commit()?;

        self.cache.last_height = block.height - 1;

        Ok(())
    }

    /// Add block to core schema only
    pub fn include_block_core_only(&self, block: &BlockData) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut tx = client.transaction()?;
        core::include_block(&mut tx, block)?;
        tx.commit()?;
        Ok(())
    }

    /// Bootstraps non-core tables and reloads db cache.
    ///
    /// Bootstrapped schema's are skipped automatically.
    pub fn bootstrap_derived_schemas(&mut self) -> anyhow::Result<()> {
        debug!("Local work mem: {}", &self.bootstrapping_work_mem_kb);

        /// Configures db transaction for each bootstrap function.
        fn run(db: &DB, f: &dyn Fn(&mut Transaction) -> anyhow::Result<()>) -> anyhow::Result<()> {
            let mut client = Client::connect(&db.conn_str, NoTls)?;
            let mut tx = client.transaction()?;
            tx.execute(
                &format!("set local work_mem = {};", db.bootstrapping_work_mem_kb),
                &[],
            )
            .unwrap();
            f(&mut tx)?;
            tx.commit()?;
            Ok(())
        }

        let mut client = Client::connect(&self.conn_str, NoTls)?;

        run(&self, &unspent::bootstrap)?;
        addresses::bootstrap(&mut client)?;
        blocks::bootstrap(&mut client)?;
        run(&self, &cexs::bootstrap)?;
        metrics::bootstrap(&mut client, self.bootstrapping_work_mem_kb)?;

        // Bootstrapping doesn't rely on cache and will not update it,
        // so reload it now to pick up latest db state.
        self.load_cache();

        Ok(())
    }

    pub fn last_coingecko_timestamp(&self) -> u64 {
        self.cache.coingecko.last_datapoint.timestamp
    }

    /// Add `timeseries` data to db
    pub fn inlcude_coingecko(
        &mut self,
        timeseries: crate::coingecko::TimeSeries,
    ) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut tx = client.transaction()?;
        coingecko::include_timeseries(&mut tx, &timeseries, &mut self.cache.coingecko)?;
        tx.commit()?;
        Ok(())
    }

    pub fn periodic_cex_deposits_processing(
        &mut self,
        interval: i32,
        buffer: i32,
    ) -> anyhow::Result<()> {
        let gap = self.cache.last_height - self.cache.cexs.deposit_addresses.last_processed_height;
        if gap >= interval + buffer {
            // Processing range from next height up to highest multiple of interval
            let diff = self.cache.last_height - buffer;
            let max_height = diff - diff % interval;

            let mut client = Client::connect(&self.conn_str, NoTls)?;
            let mut tx = client.transaction()?;

            let queues = cexs::deposit_addresses::spot(
                &mut tx,
                max_height,
                &self.cache.cexs.deposit_addresses,
            );
            self.process_deposit_address_queues(&mut tx, queues);
            tx.commit()?;

            self.cache.cexs.deposit_addresses.last_processed_height = max_height;
        }
        Ok(())
    }

    fn process_deposit_address_queues(&mut self, tx: &mut Transaction, queues: AddressQueues) {
        info!(
            "Processing deposit address changes: {} new & {} excluded",
            queues.propagate.len(),
            queues.purge.len()
        );
        cexs::process_deposit_addresses(tx, &queues, &mut self.cache.cexs);
        metrics::process_deposit_addresses(tx, &queues);
    }
}

impl DB {
    pub fn new(
        host: &str,
        port: u16,
        name: &str,
        user: &str,
        pass: &str,
        bootstrapping_work_mem_kb: u32,
    ) -> Self {
        debug!(
            "Creating DB instance with host: {}, port: {}, name: {}, user: {}, pass: *...*",
            host, port, name, user
        );
        let conn_str = format!(
            "host={} port={} dbname={} user={} password={}",
            host, port, name, user, pass
        );
        DB {
            conn_str,
            bootstrapping_work_mem_kb,
            buffer: Buffer::new(),
            cache: Cache::new(),
        }
    }

    pub fn check_migrations(&self, allow_migrations: bool) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        migrations::check(&mut client, allow_migrations)
    }

    /// Returns true if db is empty
    pub fn is_empty(&self) -> anyhow::Result<bool> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        // Genesis boxes will be the first thing to be included,
        // before any headers, so check for presence of outputs
        let row = client.query_one(
            "select not exists (select * from core.outputs limit 1);",
            &[],
        )?;
        let empty: bool = row.get(0);
        Ok(empty)
    }

    /// Returns true if constraints were set
    pub fn has_constraints(&self) -> anyhow::Result<bool> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        // If core.headers has a PK, then constraints have been set.
        let row = client.query_one(
            "select exists(
                select *
                from pg_index i
                join pg_attribute a on a.attrelid = i.indrelid
                    and a.attnum = any(i.indkey)
                where i.indrelid = 'core.headers'::regclass
                and i.indisprimary
            );",
            &[],
        )?;
        let has: bool = row.get(0);
        Ok(has)
    }

    /// Load core constraints and indexes
    pub fn apply_core_constraints(&self) -> anyhow::Result<()> {
        info!("Loading core constraints and indexes");

        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut tx = client.transaction()?;

        core::set_constraints(&mut tx);

        tx.commit()?;
        Ok(())
    }

    /// Retrieves sync head from current db state.
    ///
    /// For an empty database, returns:
    /// ```
    /// Head {
    ///     height: 0,
    ///     header_id: "0000000000000000000000000000000000000000000000000000000000000000",
    /// }
    /// ```
    pub fn get_head(&self) -> Result<Head, postgres::Error> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        // Cast height to oid to allow deserialisation to u32
        let row_opt = client.query_opt(
            "
            select height::oid
                , id
            from core.headers
            order by 1 desc
            limit 1;",
            &[],
        )?;
        match row_opt {
            Some(row) => Ok(Head {
                height: row.get("height"),
                header_id: row.get("id"),
            }),
            None => Ok(Head {
                height: 0,
                header_id: String::from(
                    "0000000000000000000000000000000000000000000000000000000000000000",
                ),
            }),
        }
    }

    /// Checks genesis block data is present
    pub fn has_genesis_boxes(&self) -> bool {
        let mut client = Client::connect(&self.conn_str, NoTls).unwrap();
        // Outputs cannot be empty if genesis boxes have been processed
        let row = client
            .query_one("select exists (select * from core.outputs limit 1);", &[])
            .unwrap();
        let exists: bool = row.get(0);
        exists
    }

    /// Load initialized cache
    pub fn load_cache(&mut self) {
        let mut client = Client::connect(&self.conn_str, NoTls).unwrap();
        self.cache.load(&mut client);
    }
}

/// Keeps transient db state.
///
/// Sometimes, both old and new state is needed. The buffer is used
/// to store old state of data depending on it.
#[derive(Debug)]
pub struct Buffer {
    pub supply_age_diffs: metrics::supply_age::SupplyAgeDiffs,
}

impl Buffer {
    pub fn new() -> Self {
        Self {
            supply_age_diffs: metrics::supply_age::SupplyAgeDiffs::new(),
        }
    }
}

#[derive(Debug)]
pub struct Cache {
    last_height: i32,
    pub cexs: cexs::Cache,
    pub coingecko: coingecko::Cache,
    pub metrics: metrics::Cache,
    pub blocks: blocks::Cache,
}

impl Cache {
    /// Initialize a cache with default values, representing an empty database.
    pub fn new() -> Self {
        Self {
            last_height: 0,
            cexs: cexs::Cache::new(),
            coingecko: coingecko::Cache::new(),
            metrics: metrics::Cache::new(),
            blocks: blocks::Cache::new(),
        }
    }

    /// Load cache values from db
    pub fn load(&mut self, client: &mut Client) {
        info!("Loading cache");
        self.last_height = get_last_height(client);
        self.cexs = cexs::Cache::load(client);
        self.coingecko = coingecko::Cache::load(client);
        self.metrics = metrics::Cache::load(client);
        self.blocks = blocks::Cache::load(client);
    }
}

fn get_last_height(client: &mut Client) -> i32 {
    match client
        .query_one("select max(height) from core.headers;", &[])
        .unwrap()
        .get(0)
    {
        Some(h) => h,
        None => 0,
    }
}

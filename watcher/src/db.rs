pub mod balances;
pub mod core;
pub mod metrics;
mod migrations;
pub mod unspent;

use crate::session::cache;
use crate::session::cache::Cache;
use log::debug;
use log::info;
use postgres::{Client, NoTls, Transaction};

use crate::parsing::BlockData;
use crate::types::Head;

#[derive(Debug)]
pub struct DB {
    conn_str: String,
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
        // Other schemas pick up genesis boxes from core tables
        // during bootstrapping.
        tx.commit()?;
        Ok(())
    }

    /// Add block to database
    pub fn include_block(&self, block: &BlockData, cache: &mut Cache) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut tx = client.transaction()?;

        core::include_block(&mut tx, block)?;
        unspent::include_block(&mut tx, block)?;
        balances::include_block(&mut tx, block)?;
        metrics::include_block(&mut tx, block, &mut cache.metrics)?;

        tx.commit()?;

        Ok(())
    }

    /// Restore db state to what it was before including given block
    pub fn rollback_block(&self, block: &BlockData, cache: &mut Cache) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut tx = client.transaction()?;

        metrics::rollback_block(&mut tx, block, &mut cache.metrics)?;
        balances::rollback_block(&mut tx, block)?;
        unspent::rollback_block(&mut tx, block)?;
        core::rollback_block(&mut tx, block)?;

        tx.commit()?;

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

    pub fn bootstrap_derived_schemas(&self) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut tx = client.transaction()?;

        unspent::bootstrap(&mut tx)?;
        balances::bootstrap(&mut tx)?;
        metrics::bootstrap(&mut tx)?;

        tx.commit()?;

        Ok(())
    }
}

impl DB {
    pub fn new(host: &str, port: u16, name: &str, user: &str, pass: &str) -> Self {
        debug!(
            "Creating DB instance with host: {}, port: {}, name: {}, user: {}, pass: *...*",
            host, port, name, user
        );
        DB {
            conn_str: format!(
                "host={} port={} dbname={} user={} password={}",
                host, port, name, user, pass
            ),
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

    /// Return initialized cache
    pub fn load_cache(&self) -> cache::Cache {
        info!("Preparing cache");
        let mut client = Client::connect(&self.conn_str, NoTls).unwrap();
        cache::Cache {
            metrics: metrics::load_cache(&mut client),
        }
    }
}

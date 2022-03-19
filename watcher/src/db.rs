pub mod balances;
pub mod core;
mod migrations;
pub mod unspent;

use log::debug;
use log::info;
use postgres::{Client, NoTls};

use crate::types::Head;

#[derive(Debug)]
pub struct DB {
    conn_str: String,
}

pub struct ConstraintsStatus {
    pub tier_1: bool,
    pub tier_2: bool,
    pub all_set: bool,
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

    /// Returns true if db constraints are set.
    pub fn constraints_status(&self) -> anyhow::Result<ConstraintsStatus> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let row = client.query_one("select tier_1, tier_2 from ew.constraints;", &[])?;
        let tier_1 = row.get("tier_1");
        let tier_2 = row.get("tier_2");
        Ok(ConstraintsStatus {
            tier_1: tier_1,
            tier_2: tier_2,
            all_set: tier_1 && tier_2,
        })
    }

    /// Load tier 1 db constraints and indexes
    pub fn apply_constraints_tier1(&self) -> anyhow::Result<()> {
        info!("Loading tier-1 constraints and indexes");
        let statements: Vec<&'static str> = vec![
            // Core - headers
            core::sql::header::constraints::ADD_PK,
            core::sql::header::constraints::NOT_NULL_ID,
            core::sql::header::constraints::NOT_NULL_PARENT_ID,
            core::sql::header::constraints::NOT_NULL_TIMESTAMP,
            core::sql::header::constraints::UNIQUE,
            core::sql::header::constraints::UNIQUE_PARENT_ID,
            // Core - transactions
            core::sql::transaction::constraints::ADD_PK,
            core::sql::transaction::constraints::FK_HEADER_ID,
            core::sql::transaction::constraints::IDX_HEIGHT,
            // Core - outputs
            core::sql::outputs::constraints::ADD_PK,
            core::sql::outputs::constraints::NOT_NULL_TX_ID,
            core::sql::outputs::constraints::NOT_NULL_HEADER_ID,
            core::sql::outputs::constraints::NOT_NULL_ADDRESS,
            core::sql::outputs::constraints::FK_TX_ID,
            core::sql::outputs::constraints::FK_HEADER_ID,
            core::sql::outputs::constraints::IDX_TX_ID,
            core::sql::outputs::constraints::IDX_HEADER_ID,
            core::sql::outputs::constraints::IDX_ADDRESS,
            core::sql::outputs::constraints::IDX_INDEX,
            // Core - inputs
            core::sql::inputs::constraints::ADD_PK,
            core::sql::inputs::constraints::NOT_NULL_TX_ID,
            core::sql::inputs::constraints::NOT_NULL_HEADER_ID,
            core::sql::inputs::constraints::FK_TX_ID,
            core::sql::inputs::constraints::FK_HEADER_ID,
            core::sql::inputs::constraints::IDX_TX_ID,
            core::sql::inputs::constraints::IDX_HEADER_ID,
            core::sql::inputs::constraints::IDX_INDEX,
            // Core - data inputs
            core::sql::data_inputs::constraints::ADD_PK,
            core::sql::data_inputs::constraints::NOT_NULL_HEADER_ID,
            core::sql::data_inputs::constraints::FK_TX_ID,
            core::sql::data_inputs::constraints::FK_HEADER_ID,
            core::sql::data_inputs::constraints::FK_BOX_ID,
            core::sql::data_inputs::constraints::IDX_TX_ID,
            core::sql::data_inputs::constraints::IDX_HEADER_ID,
            // Core - registers
            core::sql::registers::constraints::ADD_PK,
            core::sql::registers::constraints::FK_BOX_ID,
            core::sql::registers::constraints::CHECK_ID_GE4_AND_LE_9,
            // Core - tokens
            core::sql::tokens::constraints::ADD_PK,
            core::sql::tokens::constraints::NOT_NULL_BOX_ID,
            core::sql::tokens::constraints::FK_BOX_ID,
            core::sql::tokens::constraints::CHECK_EMISSION_AMOUNT_GT0,
            // Core - box assets
            core::sql::assets::constraints::ADD_PK,
            core::sql::assets::constraints::NOT_NULL_BOX_ID,
            core::sql::assets::constraints::NOT_NULL_TOKEN_ID,
            core::sql::assets::constraints::FK_BOX_ID,
            core::sql::assets::constraints::CHECK_AMOUNT_GT0,
            // Unspent
            // PK needed during phase 2 (for delete statements)
            unspent::usp::constraints::ADD_PK,
            // ERG Balances
            // Bootstrap phase 2 relies on pk and index.
            // Delaying the check might cause intermediate negative values to go unnoticed,
            // so keeping it here
            balances::erg::constraints::ADD_PK,
            balances::erg::constraints::CHECK_VALUE_GE0,
            balances::erg::constraints::IDX_VALUE,
            // ERG balance diffs
            // Both PK and index needed by erg.bal bootstrap queries
            balances::erg_diffs::constraints::ADD_PK,
            balances::erg_diffs::constraints::IDX_HEIGHT,
            // Token Balances

            // Finally
            "update ew.constraints set tier_1 = true;",
        ];

        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut transaction = client.transaction()?;
        for statement in statements {
            transaction.execute(statement, &[])?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Load tier 2 db constraints and indexes
    pub fn apply_constraints_tier2(&self) -> anyhow::Result<()> {
        info!("Loading tier-2 constraints and indexes");
        let statements: Vec<&'static str> = vec![
            // Finally
            "update ew.constraints set tier_1 = true;",
        ];

        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut transaction = client.transaction()?;
        for statement in statements {
            transaction.execute(statement, &[])?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Load all constraints and indexes
    ///
    /// Used when skipping bootstrap process.
    pub fn apply_constraints_all(&self) -> anyhow::Result<()> {
        self.apply_constraints_tier1()?;
        self.apply_constraints_tier2()?;
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

    /// Get sync height of derived (i.e. non-core) tables
    ///
    /// Will be different from core tables during bootstrapping process.
    pub fn get_bootstrap_height(&self) -> Result<i32, postgres::Error> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        // Cast height to oid to allow deserialisation to u32
        let row_opt = client.query_opt(
            "
            select height
            from bal.erg_diffs
            order by 1 desc
            limit 1;",
            &[],
        )?;
        match row_opt {
            Some(row) => Ok(row.get("height")),
            None => Ok(0),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum SQLArg {
    SmallInt(i16),
    Integer(i32),
    BigInt(i64),
    Text(String),
}

/// Stores a SQL statement and its arguments.
#[derive(Debug, PartialEq)]
pub struct SQLStatement {
    pub sql: String, // ToDo use &str instead
    pub args: Vec<SQLArg>,
}

impl SQLStatement {
    pub fn execute(&self, tx: &mut postgres::Transaction) -> Result<(), postgres::Error> {
        // https://stackoverflow.com/questions/37797242/how-to-get-a-slice-of-references-from-a-vector-in-rust
        let arg_refs = &self
            .args
            .iter()
            .map(|arg| match arg {
                SQLArg::SmallInt(v) => v as &(dyn postgres::types::ToSql + Sync),
                SQLArg::Integer(v) => v as &(dyn postgres::types::ToSql + Sync),
                SQLArg::Text(v) => v as &(dyn postgres::types::ToSql + Sync),
                SQLArg::BigInt(v) => v as &(dyn postgres::types::ToSql + Sync),
            })
            .collect::<Vec<&(dyn postgres::types::ToSql + Sync)>>();
        tx.execute(&self.sql, &arg_refs[..])?;
        Ok(())
    }
}

impl From<&'static str> for SQLStatement {
    fn from(query: &'static str) -> Self {
        SQLStatement {
            sql: String::from(query),
            args: vec![],
        }
    }
}

impl DB {
    pub fn execute_in_transaction(
        &self,
        statements: Vec<SQLStatement>,
    ) -> Result<(), postgres::Error> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut transaction = client.transaction()?;
        for stmt in statements {
            stmt.execute(&mut transaction)?;
        }
        transaction.commit()
    }
}

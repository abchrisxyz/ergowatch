pub mod balances;
pub mod core;
mod migrations;
pub mod unspent;

use log::debug;
use postgres::{Client, NoTls};

use crate::types::Head;

#[derive(Debug)]
pub struct DB {
    conn_str: String,
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
    pub fn has_constraints(&self) -> anyhow::Result<bool> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let row = client.query_one("select constraints_set from ew.revision;", &[])?;
        let set: bool = row.get("constraints_set");
        Ok(set)
    }

    pub fn apply_constraints(&self, sql: String) -> anyhow::Result<()> {
        let mut client = Client::connect(&self.conn_str, NoTls)?;
        let mut transaction = client.transaction()?;
        for statement in sql.split(";") {
            transaction.execute(statement.trim(), &[])?;
        }
        transaction.commit()?;
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

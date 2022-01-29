pub mod core;

use postgres::{Client, NoTls};

use crate::types::Head;

#[derive(Debug)]
pub struct DB<'a> {
    host: &'a str,
    port: u16,
    name: &'a str,
    user: &'a str,
    pass: &'a str,
}

impl<'a> DB<'a> {
    pub fn new(host: &'a str, port: u16, name: &'a str, user: &'a str, pass: &'a str) -> Self {
        DB {
            host: host,
            port: port,
            name: name,
            user: user,
            pass: pass,
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

impl DB<'_> {
    pub fn execute_in_transaction(
        &self,
        statements: Vec<SQLStatement>,
    ) -> Result<(), postgres::Error> {
        let mut client = Client::connect(
            &format!(
                "host={} port={} dbname={} user={} password={}",
                self.host, self.port, self.name, self.user, self.pass
            ),
            NoTls,
        )?;
        let mut transaction = client.transaction()?;
        for stmt in statements {
            stmt.execute(&mut transaction)?;
        }
        transaction.commit()
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
        let mut client = Client::connect(
            &format!(
                "host={} port={} dbname={} user={} password={}",
                self.host, self.port, self.name, self.user, self.pass
            ),
            NoTls,
        )?;
        // Cast height to oid to allow deserialisation to u32
        let row_opt = client.query_opt(
            "\
            select height::oid \
                , id \
            from core.headers \
            order by 1 desc \
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
}

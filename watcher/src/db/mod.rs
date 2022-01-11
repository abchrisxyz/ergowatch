pub mod core;

use postgres::{Client, NoTls};

use crate::types::Head;

pub enum Statement {
    Core(core::CoreStatement),
}

impl Statement {

    // pub fn execute(&self) -> Result<(), postgres::Error> {
    //     // ToDo: factor connection out of here !!!
    //     let mut client = Client::connect(
    //         "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
    //         NoTls,
    //     )?;
    //     let stmt = match self {
    //         Statement::InsertHeader(stmt) => stmt,
    //     };
    //     stmt.execute(&mut client)
    // }

    pub fn execute(&self) -> Result<(), postgres::Error> {
        // ToDo: factor connection out of here !!!
        let mut client = Client::connect(
            "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
            NoTls,
        )?;
        match self {
            Statement::Core(stmt) => stmt,
        }.execute(&mut client)
    }
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
pub fn get_head() -> Result<Head, postgres::Error> {
    let mut client = Client::connect(
        "host=192.168.1.72 port=5432 dbname=dev user=ergo password=ergo",
        NoTls,
    )?;
    // Cast height to oid to allow deserialisation to u32
    let row_opt = client.query_opt("\
        select height::oid \
            , id \
        from core.headers \
        order by 1 desc \
        limit 1;",
        &[]
    )?;
    match row_opt {
        Some(row) => Ok(Head {
            height: row.get("height"),
            header_id: row.get("id"),
        }), 
        None => Ok(Head {
            height: 0,
            header_id: String::from("0000000000000000000000000000000000000000000000000000000000000000"),
        })   
    }
}

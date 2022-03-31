use super::SQLArg;
use super::SQLStatement;
use anyhow::anyhow;
use log::info;
use postgres::Client;

const CURRENT_VERSION: i32 = 2;

/// Check db version and apply migrations if needed.
pub fn check(client: &mut Client, allow_migrations: bool) -> anyhow::Result<()> {
    let db_version = get_db_version(client)?;
    info!("Current db version is: {}", db_version);

    if db_version > CURRENT_VERSION {
        return Err(anyhow!(
            "Database was created by a more recent version of this program."
        ));
    } else if db_version == CURRENT_VERSION {
        info!("Database version is up to date");
        return Ok(());
    }

    let diff = CURRENT_VERSION - db_version;
    if !allow_migrations {
        return Err(anyhow!("Database is {} revision(s) behind. Run with the -m option to allow migrations to be applied.", diff));
    }
    // Migration ID = revision - 1 (i.e. migration 1 results in revision 2)
    for mig_id in db_version..(CURRENT_VERSION) {
        apply_migration(client, mig_id)?;
    }
    Ok(())
}

/// Retrieves current schema version.
fn get_db_version(client: &mut Client) -> Result<i32, postgres::Error> {
    let row = client.query_one("select version from ew.revision;", &[])?;
    Ok(row.get("version"))
}

/// Retrieves current schema version.
fn apply_migration(client: &mut Client, migration_id: i32) -> Result<(), postgres::Error> {
    info!(
        "Applying migration {} (revision {})",
        migration_id,
        migration_id + 1
    );
    match migration_id {
        1 => mig_001(client),
        _ => panic!("Attempted to apply migration with unknown ID"),
    }
}

/// Migration 1
///
/// Adds mtr schema and mtr.utxos table.
fn mig_001(client: &mut Client) -> Result<(), postgres::Error> {
    //  Get current db height
    let row = client.query_one(
        "select height from core.headers order by 1 desc limit 1;",
        &[],
    )?;
    let sync_height: i32 = row.get(0);

    // Start height should be zero, but could be different in some mocked up context.
    let row = client.query_one("select min(height) from core.headers;", &[])?;
    let start_height: i32 = row.get(0);

    info!(
        "Generating utxo counts for heights {} to {}",
        start_height, sync_height
    );

    let statements = vec![
        SQLStatement::from("set local work_mem = '32MB';"),
        SQLStatement::from("create schema mtr;"),
        SQLStatement::from("create unlogged table mtr.utxos(height int, value bigint);"),
        // Backfill procedure
        SQLStatement::from(
            "
            create procedure mtr.fill_utxos(from_h int, to_h int) as $$
            declare
                _prev bigint = 0;
                _new bigint; 
            begin
                for h in from_h .. to_h
                loop
                    select _prev + (
                        select count(*)
                        from core.outputs op
                        join core.headers hs on hs.id = op.header_id 
                        where hs.height = h
                    ) - (
                        select count(*)
                        from core.inputs op
                        join core.headers hs on hs.id = op.header_id 
                        where hs.height = h
                    ) into _new;

                    insert into mtr.utxos (height, value) values (h, _new);

                    _prev = _new;
                end loop;
            end;
            $$ language plpgsql;",
        ),
        // Call procedure
        SQLStatement {
            sql: String::from("call mtr.fill_utxos($1, $2);"),
            args: vec![SQLArg::Integer(start_height), SQLArg::Integer(sync_height)],
        },
        // Add constraints
        SQLStatement::from("alter table mtr.utxos add primary key(height);"),
        // Cleanup
        SQLStatement::from("alter table mtr.utxos set logged;"),
        SQLStatement::from("drop procedure mtr.fill_utxos;"),
        // Update revision
        SQLStatement::from("update ew.revision set version = version + 1;"),
    ];
    let mut transaction = client.transaction()?;
    for stmt in statements {
        stmt.execute(&mut transaction)?;
    }
    transaction.commit()?;
    Ok(())
}

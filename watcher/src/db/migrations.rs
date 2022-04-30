use anyhow::anyhow;
use log::info;
use postgres::Client;

const CURRENT_VERSION: i32 = 4;

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
fn apply_migration(client: &mut Client, migration_id: i32) -> anyhow::Result<()> {
    info!(
        "Applying migration {} (revision {})",
        migration_id,
        migration_id + 1
    );
    match migration_id {
        1 => mig_001(client),
        2 => mig_002(client),
        3 => mig_003(client),
        _ => Err(anyhow!("Attempted to apply migration with unknown ID")),
    }
}

/// Migration 1
///
/// Adds mtr schema and mtr.utxos table.
fn mig_001(client: &mut Client) -> anyhow::Result<()> {
    let mut tx = client.transaction()?;

    tx.execute("set local work_mem = '32MB';", &[])?;
    tx.execute("create schema mtr;", &[])?;
    tx.execute("create table mtr.utxos(height int, value bigint);", &[])?;

    super::metrics::utxos::bootstrap(&mut tx)?;

    // Update revision
    tx.execute("update ew.revision set version = version + 1;", &[])?;

    tx.commit()?;
    Ok(())
}

/// Migration 2
///
/// Fixes genesis box timestamps
fn mig_002(client: &mut Client) -> anyhow::Result<()> {
    let mut tx = client.transaction()?;
    tx.execute(
        "update core.headers set timestamp = 1561978800000 where height = 0",
        &[],
    )?;
    tx.execute("update ew.revision set version = version + 1;", &[])?;
    tx.commit()?;
    Ok(())
}

/// Migration 3
///
/// Drop contraints table
fn mig_003(client: &mut Client) -> anyhow::Result<()> {
    let mut tx = client.transaction()?;
    tx.execute("drop table ew.constraints cascade;", &[])?;
    tx.execute("update ew.revision set version = version + 1;", &[])?;
    tx.commit()?;
    Ok(())
}

use anyhow::anyhow;
use log::info;
use postgres::Client;

mod mig_001;
mod mig_002;
mod mig_003;
mod mig_004;
mod mig_005;
mod mig_006;

// Version 1 was originial schema
const CURRENT_VERSION: i32 = 7;

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
    let mut tx = client.transaction()?;
    match migration_id {
        1 => mig_001::apply(&mut tx)?,
        2 => mig_002::apply(&mut tx)?,
        3 => mig_003::apply(&mut tx)?,
        4 => mig_004::apply(&mut tx)?,
        5 => mig_005::apply(&mut tx)?,
        6 => mig_006::apply(&mut tx)?,
        _ => return Err(anyhow!("Attempted to apply migration with unknown ID")),
    };
    // Increment revision
    tx.execute("update ew.revision set version = version + 1;", &[])?;
    tx.commit()?;
    Ok(())
}

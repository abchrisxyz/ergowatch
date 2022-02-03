use anyhow::anyhow;
use log::info;
use postgres::Client;

const CURRENT_VERSION: i32 = 1;

/// Check db version and apply migrations if needed.
pub fn check(client: &mut Client, allow_migrations: bool) -> anyhow::Result<()> {
    let db_version = get_db_version(client)?;
    info!("Current db version is: {}", db_version);

    if db_version > CURRENT_VERSION {
        return Err(anyhow!(
            "Database was created by a more recent version of this program."
        ));
    } else if db_version == CURRENT_VERSION {
        info!("Database is up to date");
        return Ok(());
    }

    let diff = CURRENT_VERSION - db_version;
    if !allow_migrations {
        return Err(anyhow!("Database is {} revision(s) behind. Run with the -m option to allow migrations to be applied.", diff));
    }
    for mig_id in db_version..(CURRENT_VERSION + 1) {
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
fn apply_migration(_client: &mut Client, migration_id: i32) -> Result<(), postgres::Error> {
    info!("Applying migration {}", migration_id);
    // TODO implement when first migration is ready
    unimplemented!();
    // Ok(())
}

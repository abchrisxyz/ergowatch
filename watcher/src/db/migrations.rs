use anyhow::anyhow;
use log::info;
use postgres::Client;

mod mig_001;
mod mig_002;
mod mig_003;
mod mig_004;
mod mig_005;
mod mig_006;
mod mig_007;
mod mig_008;
mod mig_009;
mod mig_010;
mod mig_011;
mod mig_012;
mod mig_013;
mod mig_014;
mod mig_015;

const CURRENT_REVISION_MAJOR: i32 = 2;
const CURRENT_REVISION_MINOR: i32 = 15;

struct Revision {
    major: i32,
    minor: i32,
}

/// Check db version and apply migrations if needed.
pub fn check(client: &mut Client, allow_migrations: bool) -> anyhow::Result<()> {
    let rev = get_db_revision(client)?;
    info!("Current db revision: {}.{}", rev.major, rev.minor);

    if rev.major > CURRENT_REVISION_MAJOR || rev.minor > CURRENT_REVISION_MINOR {
        return Err(anyhow!(
            "Database was created by a more recent version of this program."
        ));
    } else if rev.major < CURRENT_REVISION_MAJOR {
        return Err(anyhow!(
            "Migrations are not supported for major revision changes."
        ));
    } else if rev.minor == CURRENT_REVISION_MINOR {
        info!("Database revision is up to date");
        return Ok(());
    }

    let diff = CURRENT_REVISION_MINOR - rev.minor;
    if !allow_migrations {
        return Err(anyhow!("Database is {} revision(s) behind. Run with the -m option to allow migrations to be applied.", diff));
    }

    for mig_id in rev.minor + 1..(CURRENT_REVISION_MINOR + 1) {
        apply_migration(client, mig_id)?;
    }
    Ok(())
}

/// Retrieves current schema version.
fn get_db_revision(client: &mut Client) -> Result<Revision, postgres::Error> {
    let row = client.query_one("select major, minor from ew.revision;", &[])?;
    Ok(Revision {
        major: row.get("major"),
        minor: row.get("minor"),
    })
}

/// Retrieves current schema version.
fn apply_migration(client: &mut Client, migration_id: i32) -> anyhow::Result<()> {
    info!("Applying migration {}", migration_id,);
    let mut tx = client.transaction()?;
    match migration_id {
        1 => mig_001::apply(&mut tx)?,
        2 => mig_002::apply(&mut tx)?,
        3 => mig_003::apply(&mut tx)?,
        4 => mig_004::apply(&mut tx)?,
        5 => mig_005::apply(&mut tx)?,
        6 => mig_006::apply(&mut tx)?,
        7 => mig_007::apply(&mut tx)?,
        8 => mig_008::apply(&mut tx)?,
        9 => mig_009::apply(&mut tx)?,
        10 => mig_010::apply(&mut tx)?,
        11 => mig_011::apply(&mut tx)?,
        12 => mig_012::apply(&mut tx)?,
        13 => mig_013::apply(&mut tx)?,
        14 => mig_014::apply(&mut tx)?,
        15 => mig_015::apply(&mut tx)?,
        _ => return Err(anyhow!("Attempted to apply migration with unknown ID")),
    };
    // Increment revision
    tx.execute("update ew.revision set minor = minor + 1;", &[])?;
    tx.commit()?;
    Ok(())
}

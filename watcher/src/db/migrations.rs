use anyhow::anyhow;
use log::info;
use postgres::Client;

const CURRENT_VERSION: i32 = 5;

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
        4 => mig_004(client),
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

/// Migration 4
///
/// Add main exchange addresses
fn mig_004(client: &mut Client) -> anyhow::Result<()> {
    let mut tx = client.transaction()?;
    tx.execute("create schema cex;", &[])?;
    tx.execute("create table cex.cexs ( id integer, name text );", &[])?;
    tx.execute(
        "create type cex.t_address_type as enum ('main', 'deposit');",
        &[],
    )?;
    tx.execute(
        "create table cex.addresses (
            address text,
            cex_id integer,
            type cex.t_address_type,
            spot_height integer
        );",
        &[],
    )?;
    tx.execute(
        "create type cex.t_block_status as enum (
            'pending',
            'pending_rollback',
            'processing',
            'processing_rollback',
            'processed',
            'processed_rollback'
        );",
        &[],
    )?;
    tx.execute(
        "create table cex.block_processing_log (
            header_id text,
            height integer,
            invalidation_height integer,
            status cex.t_block_status not null
        );",
        &[],
    )?;
    tx.execute(
        "insert into cex.cexs (id, name) values
            (1, 'Coinex'),
            (2, 'Gate.io'),
            (3, 'KuCoin'),
            (4, 'ProBit')
        ;",
        &[],
    )?;
    tx.execute(
        "insert into cex.addresses (cex_id, type, address) values
            -- Coinex
            (1, 'main', '9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU'),
            (1, 'main', '9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe'),
                 
            -- Gate - confirmed
            (2, 'main', '9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC'),
            -- Gate - unconfirmed
            (2, 'main', '9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE'),
            (2, 'main', '9i7134eY3zUotQyS8nBeZDJ3SWbTPn117nCJYi977FBn9AaxhZY'),
            (2, 'main', '9gmb745thQTyoGGWxSr9hNmvipivgVbQGA6EJnBucs3nwi9yqoc'),
            (2, 'main', '9fJzuyVaRLM9Q3RZVzkau1GJVP9TDiW8GRL5p25VZ8VNXurDpaw'),
            (2, 'main', '9i1ETULiCnGMtppDAvrcYujhxX18km3ge9ZEDMnZPN6LFQbttRF'),
            (2, 'main', '9gck4LwHJK3XV2wXdYdN5S9Fe4RcFrkaqs4WU5aeiKuodJyW7qq'),
            (2, 'main', '9gv4qw7RtQyt3khtnQNxp7r7yuUazWWyTGfo7duqGj9hMtZxKP1'),
            -- created 10/2019, but listing only in 10/2020 ?
            (2, 'main', '9exS2B892HTiDkqhcWnj1nzsbYmVn7ameVb1d2jagUWTqaLxfTX'),

            -- KuCoin
            (3, 'main', '9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu'),
            (3, 'main', '9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3'),
            (3, 'main', '9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr'),
            (3, 'main', '9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT'),
            
            -- ProBit
            (4, 'main', '9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB')
        ;",
        &[],
    )?;

    tx.commit()?;
    Ok(())
}

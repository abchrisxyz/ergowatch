/// Migration 28
///
/// Add repair heights to ew.repairs
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("drop table if exists ew.repairs;", &[])?;
    tx.execute(
        "
        create table ew.repairs (
            singleton int primary key default 1,
            started timestamp not null,
            from_height int not null,
            last_height int not null,
            next_height int not null,
            check(singleton = 1),
            check(last_height >= from_height),
            check(next_height >= from_height),
            check(next_height <= last_height)
        );",
        &[],
    )?;
    Ok(())
}

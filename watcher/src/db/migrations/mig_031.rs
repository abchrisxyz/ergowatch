/// Migration 31
///
/// Fix early supply age records with unhandled zero timestamps
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    //  At height 0, all ages are 0
    tx.execute(
        "
        update mtr.supply_age_days
        set overall = 0
            , p2pks = 0
            , cexs = 0
            , contracts = 0
            , miners = 0
        where height = 0;",
        &[],
    )?;

    // No p2pk supply for first 3354 blocks
    tx.execute(
        "
        update mtr.supply_age_days d
        set p2pks = 0
        from mtr.supply_age_timestamps t
        where t.height = d.height
            and t.height <= 3354
            and t.p2pks = 0;",
        &[],
    )?;

    // No main cex supply for first 81925 blocks
    tx.execute(
        "
        update mtr.supply_age_days d
        set cexs = 0
        from mtr.supply_age_timestamps t
        where t.height = d.height
            and t.height <= 81925
            and t.cexs = 0;",
        &[],
    )?;

    // No contracts supply for first 38125 blocks
    tx.execute(
        "
        update mtr.supply_age_days d
        set contracts = 0
        from mtr.supply_age_timestamps t
        where t.height = d.height
            and t.height <= 38125
            and t.contracts = 0;",
        &[],
    )?;
    Ok(())
}

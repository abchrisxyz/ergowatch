use super::Cache;
use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::Transaction;

/// Record cex supply changes
pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    let diffs = get_supply_diffs(tx, block.height);
    let statement = tx
        .prepare_typed(
            "
            insert into cex.supply (height, cex_id, main, deposit)
            values ($1, $2, $3, $4);",
            &[Type::INT4, Type::INT4, Type::INT8, Type::INT8],
        )
        .unwrap();
    for (cex_id, main_diff, deposit_diff) in diffs {
        // Update cache
        *cache.main_supply.entry(cex_id).or_insert(0) += main_diff;
        *cache.deposit_supply.entry(cex_id).or_insert(0) += deposit_diff;
        // Update db
        tx.execute(
            &statement,
            &[
                &block.height,
                &cex_id,
                &cache.main_supply[&cex_id],
                &cache.deposit_supply[&cex_id],
            ],
        )
        .unwrap();
    }
}

/// Remove deposit addresses
pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    tx.execute(
        "delete from cex.supply where height = $1;",
        &[&block.height],
    )
    .unwrap();

    // Update cache
    let diffs = get_supply_diffs(tx, block.height);
    for (cex_id, main_diff, deposit_diff) in diffs {
        *cache.main_supply.get_mut(&cex_id).unwrap() -= main_diff;
        *cache.deposit_supply.get_mut(&cex_id).unwrap() -= deposit_diff;
    }
}

pub(super) fn repair(tx: &mut Transaction, height: i32, cache: &mut Cache) {
    // Remove any existing records at height
    tx.execute("delete from cex.supply where height = $1;", &[&height])
        .unwrap();

    // Add new ones
    let diffs = get_supply_diffs(tx, height);
    let statement = tx
        .prepare_typed(
            "
            insert into cex.supply (height, cex_id, main, deposit)
            values ($1, $2, $3, $4);",
            &[Type::INT4, Type::INT4, Type::INT8, Type::INT8],
        )
        .unwrap();
    for (cex_id, main_diff, deposit_diff) in diffs {
        // Update cache
        *cache.main_supply.entry(cex_id).or_insert(0) += main_diff;
        *cache.deposit_supply.entry(cex_id).or_insert(0) += deposit_diff;
        // Update db
        tx.execute(
            &statement,
            &[
                &height,
                &cex_id,
                &cache.main_supply[&cex_id],
                &cache.deposit_supply[&cex_id],
            ],
        )
        .unwrap();
    }
}

/// Return supply diffs for cex's with supply changes at given height.
///
/// Returns tuples of (cex_id, main_diff, supply_diff).
fn get_supply_diffs(tx: &mut Transaction, height: i32) -> Vec<(i32, i64, i64)> {
    let rows = tx
        .query(
            "
            select cas.cex_id
                , coalesce(sum(dif.value) filter (where cas.type = 'main'), 0)::bigint as main
                , coalesce(sum(dif.value) filter (where cas.type = 'deposit'), 0)::bigint as deposit
            from cex.addresses cas
            join bal.erg_diffs dif
                on dif.address = cas.address
            where dif.height = $1
            group by 1;
            ",
            &[&height],
        )
        .unwrap();

    rows.iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect()
}

use super::Cache;
use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::Transaction;

/*
    At current height (h) we have a set of known deposit addresses,
    discovered in blocks prior to h, and a set of new deposit
    addresses, discovered at h.
    Supply on deposit addresses at current height (h) is calculated as:
        S(h) = S_k(h-1) + D_k(h) + S_n(h)

    with:
        S(h)     supply in exchange deposit addresses at current height
        S_k(h-1) supply in known deposit addresses at h-1
        D_k(h)   supply diffs from known addresses at h
        S_n(h)   supply in new addresses at h

    S_k(h-1) is known from the previous block and cached.
    D_k is the sum of balance diffs linked to known deposit addresses
    at height h (table adr.erg_diffs).
    S_n(h) is the sum of balances linked to new deposit addresses (table
    erg.bal).

    Advantages of this method are:
        - adr.erg_diffs table only needs to be read for current h
        - adr.erg table only needs to be read for new addresses (if any)
        - doesn't require a join on all deposit addresses
        - latest value based on all available data (known deposit addresses)

    The drawback is that it creates a discontinuity between current and
    previous supply values if there were any new deposit addresses in
    the current block. However, the discontinuity is only temporary and
    will be fixed in the next repair event.

    Supply on main addresses is much simpler since new main addresses
    only get added through migrations. It is the cached value from last
    height plus any balance diffs linked to main addresses at current
    height:

        S(h) = S(h-1) + D(h)
*/

/// Main and deposit supply differences, in nano ERG.
struct SupplyDiff {
    cex_id: i32,
    main: i64,
    deposit: i64,
}

/// Record cex supply changes
pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    let supply_diffs = get_supply_diffs(tx, block.height);
    let statement = tx
        .prepare_typed(
            "
            insert into cex.supply (height, cex_id, main, deposit)
            values ($1, $2, $3, $4);",
            &[Type::INT4, Type::INT4, Type::INT8, Type::INT8],
        )
        .unwrap();

    for sd in supply_diffs {
        // Update cache
        *cache.main_supply.entry(sd.cex_id).or_insert(0) += sd.main;
        *cache.deposit_supply.entry(sd.cex_id).or_insert(0) += sd.deposit;
        // Update db
        tx.execute(
            &statement,
            &[
                &block.height,
                &sd.cex_id,
                &cache.main_supply[&sd.cex_id],
                &cache.deposit_supply[&sd.cex_id],
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
    let supply_diffs = get_supply_diffs(tx, block.height);
    for sd in supply_diffs {
        *cache.main_supply.get_mut(&sd.cex_id).unwrap() -= sd.main;
        *cache.deposit_supply.get_mut(&sd.cex_id).unwrap() -= sd.deposit;
    }
}

pub(super) fn repair(tx: &mut Transaction, height: i32, cache: &mut Cache) {
    // Remove any existing records at height
    tx.execute("delete from cex.supply where height = $1;", &[&height])
        .unwrap();

    // Add new ones
    let supply_diffs = repair::get_supply_diffs(tx, height);
    let statement = tx
        .prepare_typed(
            "
            insert into cex.supply (height, cex_id, main, deposit)
            values ($1, $2, $3, $4);",
            &[Type::INT4, Type::INT4, Type::INT8, Type::INT8],
        )
        .unwrap();
    for sd in supply_diffs {
        // Update cache
        *cache.main_supply.entry(sd.cex_id).or_insert(0) += sd.main;
        *cache.deposit_supply.entry(sd.cex_id).or_insert(0) += sd.deposit;
        // Update db
        tx.execute(
            &statement,
            &[
                &height,
                &sd.cex_id,
                &cache.main_supply[&sd.cex_id],
                &cache.deposit_supply[&sd.cex_id],
            ],
        )
        .unwrap();
    }
}

/// Non-zero supply diffs, by cex, relative to supply at previous height.
fn get_supply_diffs(tx: &mut Transaction, height: i32) -> Vec<SupplyDiff> {
    let rows = tx
        .query(
            "
            with known_addresses_diffs as (
                select a.cex_id
                    , coalesce(sum(d.value) filter (where a.type = 'main'), 0)::bigint as main
                    , coalesce(sum(d.value) filter (where a.type = 'deposit'), 0)::bigint as deposit
                from cex.addresses a
                join adr.erg_diffs d
                    on d.address_id = a.address_id
                where d.height = $1
                    -- exclude addresses discoverd in current block
                    -- and include main addresses explicitly since they 
                    -- have no spot_height
                    and (a.type = 'main' or a.spot_height <= $1 - 1)
                group by 1
            ), new_deposit_addresses_balances as (
                select a.cex_id
                    , sum(b.value) as deposit
                from cex.addresses a
                join adr.erg b on b.address_id = a.address_id
                where a.type = 'deposit'
                    and a.spot_height = $1
                group by 1
            )
            select d.cex_id
                , d.main::bigint
                , d.deposit + coalesce(b.deposit, 0)::bigint
            from known_addresses_diffs d
            left join new_deposit_addresses_balances b
                on b.cex_id = d.cex_id;
            ",
            &[&height],
        )
        .unwrap();

    rows.iter()
        .map(|r| SupplyDiff {
            cex_id: r.get(0),
            main: r.get(1),
            deposit: r.get(2),
        })
        .filter(|sd| sd.main != 0 || sd.deposit != 0)
        .collect()
}

mod repair {
    use super::SupplyDiff;
    use postgres::Transaction;

    /// Non-zero supply diffs, by cex, relative to supply at previous height.
    ///
    /// Repair variant not distinguishing between known/new addresses.
    pub(super) fn get_supply_diffs(tx: &mut Transaction, height: i32) -> Vec<SupplyDiff> {
        let rows = tx
            .query(
                "
                select a.cex_id
                    , coalesce(sum(d.value) filter (where a.type = 'main'), 0)::bigint as main
                    , coalesce(sum(d.value) filter (where a.type = 'deposit'), 0)::bigint as deposit
                from cex.addresses a
                join adr.erg_diffs d
                    on d.address_id = a.address_id
                where d.height = $1
                group by 1
            ",
                &[&height],
            )
            .unwrap();

        rows.iter()
            .map(|r| SupplyDiff {
                cex_id: r.get(0),
                main: r.get(1),
                deposit: r.get(2),
            })
            .filter(|sd| sd.main != 0 || sd.deposit != 0)
            .collect()
    }
}

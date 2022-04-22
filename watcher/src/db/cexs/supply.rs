use crate::parsing::BlockData;
use postgres::Transaction;

/*
Cache cex main and deposit totals.
*/

/// Find new deposit addresses
pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    // let diffs = get_supply_diffs(tx, block.height);
    // let statement = tx
    //     .prepare_typed(
    //         "
    //     insert into cex.supply (height, cex_id, main, deposit)
    //     values ($1, $2, $3, $4);",
    //         &[Type::INT4, Type::INT4, Type::INT8, Type::INT8],
    //     )
    //     .unwrap();
    // for (cex_id, main_diff, deposit_diff) in diffs {
    //     // Update cache
    //     *cache.main_supply.get_mut(&cex_id).unwrap() += main_diff;
    //     *cache.deposit_supply.get_mut(&cex_id).unwrap() += deposit_diff;
    //     // Update db
    //     tx.execute(
    //         &statement,
    //         &[
    //             &block.height,
    //             &cex_id,
    //             &cache.main_supply[&cex_id],
    //             &cache.deposit_supply[&cex_id],
    //         ],
    //     )
    //     .unwrap();
    // }

    tx.execute(
        "
        with cex_addresses as (
            select cex_id, address, type from cex.addresses
            union
            select cex_id, address, 'deposit' as type from cex.new_deposit_addresses
        ), new_balances as (
            select cex_id
                , sum(bal.value) filter (where cas.type = 'main') as main
                , sum(bal.value) filter (where cas.type = 'deposit') as deposit
            from bal.erg bal
            join cex_addresses cas on cas.address = bal.address
            group by 1
        ), current_balances as (
            select cex_id
                , main
                , deposit
            from cex.supply
            where (cex_id, height) in (
                select cex_id
                    , max(height)
                from cex.supply
                group by 1
            )
        )
        insert into cex.supply (height, cex_id, main, deposit)
        select $1
            , new.cex_id
            , new.main
            , coalesce(new.deposit, 0)
        from new_balances new
        left join current_balances cur on cur.cex_id = new.cex_id
        where (new.main <> cur.main or cur.main is null)
            or (new.deposit is not null and new.deposit <> cur.deposit)
            or (new.deposit is not null and cur.deposit is null);
        ",
        &[&block.height],
    )
    .unwrap();
}

/// Remove deposit addresses
pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(
        "delete from cex.supply where height = $1;",
        &[&block.height],
    )
    .unwrap();

    // // Update cache
    // let diffs = get_supply_diffs(tx, block.height);
    // for (cex_id, main_diff, deposit_diff) in diffs {
    //     *cache.main_supply.get_mut(&cex_id).unwrap() -= main_diff;
    //     *cache.deposit_supply.get_mut(&cex_id).unwrap() -= deposit_diff;
    // }
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        // cexs supply
        "alter table cex.supply add primary key (height, cex_id);",
        "alter table cex.supply add foreign key (cex_id)
            references cex.cexs (id) on delete cascade;",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

// / Return supply diffs for cex's with supply changes at given height.
// /
// / Returns tuples of (cex_id, main_diff, supply_diff).
// fn get_supply_diffs(tx: &mut Transaction, height: i32) -> Vec<(i32, i64, i64)> {
//     let rows = tx
//         .query(
//             "
//             with known_diffs as (
//                 select cas.cex_id
//                     , sum(dif.value) filter (where cas.type = 'main') as main
//                     , sum(dif.value) filter (where cas.type = 'deposit') as deposit
//                 from cex.addresses cas
//                 join bal.erg_diffs dif
//                     on dif.address = cas.address
//                 where dif.height = $1
//                 group by 1
//             ), pending_diffs as (
//                 -- addresses in cex.new_deposit_addresses but discovered in previous blocks
//                 select nda.cex_id
//                     , sum(dif.value) as deposit
//                 from cex.new_deposit_addresses nda
//                 join bal.erg_diffs dif
//                     on dif.address = nda.address
//                 where dif.height = $1
//                     -- new deposit addresses are not acounted for yet in
//                     -- supply, so can't
//                     -- BUT what if partial sell (i.e. some remains on deposit)
//                     and nda.spot_height <> $1
//                 group by 1
//             ), new_diffs as (
//                 -- addresses discovered in current block
//                 -- not accounted for yet in cex.supply, so need to apply remaining bal
//                 -- as diff.
//                 select nda.cex_id
//                     , sum(bal.value) as deposit
//                 from erg.bal bal
//                 join cex.new_deposit_addresses nda
//                     on nda.address = bal.address
//                 where nda.spot_height = $1
//                 group by 1
//             ), diffs as (
//                 select coalesce(k.cex_id, n.cex_id) as cex_id
//                     , coalesce(k.main, 0::bigint) as main
//                     , coalesce(k.deposit, 0::bigint) + coalesce(n.deposit, 0::bigint) as deposit
//                 from known_diffs k
//                 full outer join new_diffs n on n.cex_id = k.cex_id
//             )
//             select cex_id
//                 , main::bigint
//                 , deposit::bigint
//             from diffs
//             where main <> 0 or deposit <> 0;
//             ",
//             &[&height],
//         )
//         .unwrap();

//     rows.iter()
//         .map(|r| (r.get(0), r.get(1), r.get(2)))
//         .collect()
// }

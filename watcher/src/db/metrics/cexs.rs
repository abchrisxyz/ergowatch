/// CEX metrics
use crate::parsing::BlockData;
use log::info;
use postgres::Transaction;

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    // Supply details
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
            from mtr.cex_supply
            where (cex_id, height) in (
                select cex_id
                    , max(height)
                from mtr.cex_supply
                group by 1
            )
        )
        insert into mtr.cex_supply_details (height, cex_id, main, deposit)
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

    // Total supply
    tx.execute(
        "
        insert into mtr.cex_supply (height, total, deposit)
        select $1
            , sum(main + deposit) as total
            , sum(deposit) as deposit
        from mtr.cex_supply_details
        where (cex_id, height) in (
            select cex_id
                , max(height)
            from mtr.cex_supply_details
            group by 1
        )
        group by 1;
        ",
        &[&block.height],
    )
    .unwrap();
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    // Total supply
    tx.execute(
        "delete from mtr.cex_supply where height = $1;",
        &[&block.height],
    )
    .unwrap();

    // Supply details
    tx.execute(
        "delete from mtr.cex_supply_details where height = $1;",
        &[&block.height],
    )
    .unwrap();
}

pub fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    // // Bootstrap supply
    // let sql = "
    //     with diffs as (
    //         select cas.cex_id
    //             , dif.height
    //             , sum(dif.value) filter (where cas.type = 'main') as main
    //             , sum(dif.value) filter (where cas.type = 'deposit') as deposit
    //         from cex.addresses cas
    //         join bal.erg_diffs dif on dif.address = cas.address
    //         group by 1, 2
    //         having sum(dif.value) filter (where cas.type = 'main') <> 0
    //             or sum(dif.value) filter (where cas.type = 'deposit') <> 0
    //     )
    //     insert into cex.supply(height, cex_id, main, deposit)
    //     select
    // ";
}

fn is_bootstrapped(tx: &mut Transaction) -> bool {
    let row = tx
        .query_one("select exists(select * from mtr.cex_supply limit 1);", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        // Supply details
        "alter table mtr.cex_supply_details add primary key (height, cex_id);",
        "alter table mtr.cex_supply_details add foreign key (cex_id)
            references cex.cexs (id) on delete cascade;",
        // Total supply
        "alter table mtr.cex_supply add primary key (height);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

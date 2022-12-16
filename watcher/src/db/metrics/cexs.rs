/// CEX metrics
use crate::{db::cexs::deposit_addresses::AddressQueues, parsing::BlockData};
use log::info;
use postgres::Transaction;

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    insert_supply(tx, block.height);
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    rollback_supply(tx, block.height);
}

pub(super) fn process_deposit_addresses(tx: &mut Transaction, queues: &AddressQueues) {
    if !queues.propagate.is_empty() {
        propagate_deposit_addresses(tx, &queues.propagate);
    }
    if !queues.purge.is_empty() {
        purge_deposit_addresses(tx, &queues.purge);
    }
}

/// Add new snapshot of supply on all exchanges at `height`.
fn insert_supply(tx: &mut Transaction, height: i32) {
    tx.execute(
        "
        insert into mtr.cex_supply (height, total, deposit)
        select $1 as height
            , coalesce(sum(main + deposit), 0)
            , coalesce(sum(deposit), 0)
        from cex.supply
        where (cex_id, height) in (
            select cex_id
                , max(height)
            from cex.supply
            group by 1
        );",
        &[&height],
    )
    .unwrap();
}

/// Rremove snapshot of supply on all exchanges at `height`.
fn rollback_supply(tx: &mut Transaction, height: i32) {
    tx.execute(
        "
        delete from mtr.cex_supply
        where height = $1;",
        &[&height],
    )
    .unwrap();
}

pub fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    if is_bootstrapped(tx) {
        return Ok(());
    }
    info!("Bootstrapping metrics - exchanges");

    // Temporarily add zero balances at height zero for all cex's.
    // This will allow to compute balance diffs for each cex.
    tx.execute(
        "
        insert into cex.supply(height, cex_id, main, deposit)
        select distinct 0, cex_id, 0, 0
        from cex.supply;
        ",
        &[],
    )?;

    // Bootstrap supply
    tx.execute(
        "
        with diffs as (
            -- diffs by height and cex
            select height
                , cex_id
                , main - lag(main) over(partition by cex_id order by height) as d_main
                , deposit - lag(deposit) over(partition by cex_id order by height) as d_deposit
            from cex.supply
        ), total_diffs as (
            -- diffs by height
            select height
                , sum(d_main) as d_main
                , sum(d_deposit) as d_deposit
            from diffs
            group by 1
        )
        insert into mtr.cex_supply(height, total, deposit)
        select h.height
            , coalesce(sum(d_main + d_deposit) over(order by h.height rows between unbounded preceding and current row), 0)
            , coalesce(sum(d_deposit) over(order by h.height rows between unbounded preceding and current row), 0)
        from core.headers h
        left join total_diffs d on d.height = h.height
        order by 1;",
        &[],
    )?;

    // Remove zero balances inserted earlier.
    tx.execute("delete from cex.supply where height = 0;", &[])?;

    set_constraints(tx);
    Ok(())
}

fn is_bootstrapped(tx: &mut Transaction) -> bool {
    let row = tx
        .query_one("select exists(select * from mtr.cex_supply limit 1);", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        // Supply
        "alter table mtr.cex_supply add primary key (height);",
        "alter table mtr.cex_supply alter column height set not null;",
        "alter table mtr.cex_supply alter column total set not null;",
        "alter table mtr.cex_supply alter column deposit set not null;",
        "alter table mtr.cex_supply add check (total >= 0);",
        "alter table mtr.cex_supply add check (deposit >= 0);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

fn propagate_deposit_addresses(tx: &mut Transaction, addresses: &Vec<i64>) {
    tx.execute(
        "
        with diffs as (
            select height
                , sum(value) as value
            from adr.erg_diffs
            where address_id = any($1)
            group by 1
        ), patch as (
            select h as height
                , sum(d.value) over (
                    order by h asc
                    rows between unbounded preceding and current row
                ) as value
            from generate_series((select min(height) from diffs), (select max(height) from core.headers)) as h
            left join diffs d on d.height = h
        )
        update mtr.cex_supply s
        set total = s.total + p.value
            , deposit = s.deposit + p.value
        from patch p
        where p.height = s.height;
        ",
        &[&addresses],
    )
    .unwrap();
}

fn purge_deposit_addresses(tx: &mut Transaction, addresses: &Vec<i64>) {
    tx.execute(
        "
        with diffs as (
            select height
                , sum(value) as value
            from adr.erg_diffs
            where address_id = any($1)
            group by 1
        ), patch as (
            select h as height
                , sum(d.value) over (
                    order by h asc
                    rows between unbounded preceding and current row
                ) as value
            from generate_series((select min(height) from diffs), (select max(height) from core.headers)) as h
            left join diffs d on d.height = h
        )
        update mtr.cex_supply s
        set total = s.total - p.value
            , deposit = s.deposit - p.value
        from patch p
        where p.height = s.height;
        ",
        &[&addresses],
    )
    .unwrap();
}

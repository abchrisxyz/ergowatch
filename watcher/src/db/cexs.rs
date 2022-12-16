use crate::parsing::BlockData;
use log::debug;
use log::info;
use postgres::Client;
use postgres::Transaction;

use self::deposit_addresses::AddressQueues;

mod addresses;
pub mod deposit_addresses;
mod supply;

/// Adds address id's to cex address tables once known
pub(super) mod declaration {
    use super::addresses;
    use super::Cache;
    use crate::parsing::BlockData;
    use postgres::Transaction;

    pub fn include_block(
        tx: &mut Transaction,
        block: &BlockData,
        cache: &mut Cache,
    ) -> anyhow::Result<()> {
        if cache.unseen_main_addresses {
            addresses::declare_main_addresses(tx, cache, block.height);
        }
        if cache.unseen_ignored_addresses {
            addresses::declare_ignored_addresses(tx, cache, block.height);
        }
        Ok(())
    }

    pub fn rollback_block(
        tx: &mut Transaction,
        block: &BlockData,
        cache: &mut Cache,
    ) -> anyhow::Result<()> {
        addresses::rollback_address_declarations(tx, cache, block.height);
        Ok(())
    }
}

pub fn include_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> anyhow::Result<()> {
    supply::include(tx, block, &mut cache.supply);
    Ok(())
}

pub fn rollback_block(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> Option<AddressQueues> {
    supply::rollback(tx, block, &mut cache.supply);
    deposit_addresses::rollback(tx, block, &mut cache.deposit_addresses)
}

/// Find all deposit addresses from first to last available block.
pub fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    if is_bootstrapped(tx) {
        return Ok(());
    }
    info!("Bootstrapping CEX data (addresses)");

    // Declare main addresses
    tx.execute(
        "
        insert into cex.main_addresses (address_id, cex_id)
        select adr.id
            , lst.cex_id
        from cex.main_addresses_list lst
        join core.addresses adr on adr.address = lst.address
        order by 1;
        ",
        &[],
    )?;

    // Declare ignored addresses
    tx.execute(
        "
        insert into cex.deposit_addresses_ignored (address_id)
        select adr.id
        from cex.ignored_addresses_list lst
        join core.addresses adr on adr.address = lst.address
        order by 1;
        ",
        &[],
    )?;
    let max_height: i32 = tx
        .query_one("select max(height) from core.headers", &[])
        .unwrap()
        .get::<usize, i32>(0)
        - 1; // hard coded buffer TODO: get from config
    deposit_addresses::spot_range(tx, 0, max_height);

    // Set constraint here so that the supply query can use the indexes.
    // TODO: consider setting supply constraints later.
    set_constraints(tx);

    info!("Bootstrapping CEX data (supply)");

    // Supply
    tx.execute(
        "
        with main_diffs as (
            select d.height
                , c.cex_id
                , sum(d.value) as value
            from cex.main_addresses c
            join adr.erg_diffs d on d.address_id = c.address_id
            group by 1, 2
            having sum(d.value) <> 0
        ), deposit_diffs as (
            select d.height
                , c.cex_id
                , sum(d.value) as value
            from cex.deposit_addresses c
            join adr.erg_diffs d on d.address_id = c.address_id
            group by 1, 2
            having sum(d.value) <> 0
        ), merged as (
            select coalesce(m.height, d.height) as height
                , coalesce(m.cex_id, d.cex_id) as cex_id
                , coalesce(m.value, 0) as main
                , coalesce(d.value, 0) as deposit
            from main_diffs m
            full outer join deposit_diffs d
                on d.height = m.height
                and d.cex_id = m.cex_id
        )
        insert into cex.supply (height, cex_id, main, deposit)
            select height
                , cex_id
                , sum(main) over w as main
                , sum(deposit) over w as deposit
            from merged
            window w as (
                partition by cex_id
                order by height asc
                rows between unbounded preceding and current row
            )
            order by 1, 2;",
        &[],
    )?;
    Ok(())
}

fn is_bootstrapped(tx: &mut Transaction) -> bool {
    tx.query_one(
        "
            select last_processed_height > 0
            from cex._deposit_addresses_log;",
        &[],
    )
    .unwrap()
    .get(0)
}

fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        // cexs
        "alter table cex.cexs add primary key (id);",
        "alter table cex.cexs alter column id set not null;",
        "alter table cex.cexs alter column text_id set not null;",
        "alter table cex.cexs alter column name set not null;",
        "alter table cex.cexs add constraint cexs_unique_text_id unique (text_id);",
        "alter table cex.cexs add constraint cexs_unique_name unique (name);",
        // main addresses
        "alter table cex.main_addresses add primary key (address_id);",
        "alter table cex.main_addresses alter column address_id set not null;",
        "alter table cex.main_addresses alter column cex_id set not null;",
        "alter table cex.main_addresses add foreign key (address_id)
            references core.addresses (id);",
        "alter table cex.main_addresses add foreign key (cex_id)
            references cex.cexs (id);",
        "create index on cex.main_addresses(cex_id);",
        // deposit addresses
        "alter table cex.deposit_addresses add primary key (address_id);",
        "alter table cex.deposit_addresses alter column address_id set not null;",
        "alter table cex.deposit_addresses alter column cex_id set not null;",
        "alter table cex.deposit_addresses add foreign key (address_id)
            references core.addresses (id);",
        "alter table cex.deposit_addresses add foreign key (cex_id)
            references cex.cexs (id);",
        "create index on cex.deposit_addresses(cex_id);",
        "create index on cex.deposit_addresses(spot_height);",
        // ignored deposit addresses
        "alter table cex.deposit_addresses_ignored add primary key (address_id);",
        "alter table cex.deposit_addresses_ignored alter column address_id set not null;",
        // excluded deposit addresses
        "alter table cex.deposit_addresses_excluded add primary key (address_id);",
        "alter table cex.deposit_addresses_excluded alter column address_id set not null;",
        "alter table cex.deposit_addresses_excluded alter column address_spot_height set not null;",
        "alter table cex.deposit_addresses_excluded alter column conflict_spot_height set not null;",
        // cex.supply
        "alter table cex.supply add primary key (height, cex_id);",
        "alter table cex.supply alter column height set not null;",
        "alter table cex.supply alter column cex_id set not null;",
        "alter table cex.supply alter column main set not null;",
        "alter table cex.supply alter column deposit set not null;",
        "alter table cex.supply add foreign key (cex_id)
            references cex.cexs (id);",
        "create index on cex.supply (height);",
        "alter table cex.supply add check (main >= 0);",
        "alter table cex.supply add check (deposit >= 0);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

#[derive(Debug)]
pub struct Cache {
    pub supply: supply::Cache,
    /// Flags indicating whether all predefined addresses have been encountered
    pub unseen_main_addresses: bool,
    pub unseen_ignored_addresses: bool,
    /// Deposit addresses
    pub deposit_addresses: deposit_addresses::Cache,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            supply: supply::Cache::new(),
            unseen_main_addresses: true,
            unseen_ignored_addresses: true,
            deposit_addresses: deposit_addresses::Cache::new(),
        }
    }

    pub fn load(client: &mut Client) -> Self {
        debug!("Loading cexs cache");
        let mut c = Cache::new();
        c.supply = supply::Cache::load(client);
        let mut ro_tx = client.transaction().unwrap();
        c.unseen_main_addresses = addresses::any_unseen_main_addresses(&mut ro_tx);
        c.unseen_ignored_addresses = addresses::any_unseen_ignored_addresses(&mut ro_tx);
        ro_tx.rollback().unwrap();
        c.deposit_addresses = deposit_addresses::Cache::load(client);
        c
    }
}

pub(super) fn process_deposit_addresses(
    tx: &mut Transaction,
    queues: &deposit_addresses::AddressQueues,
    cache: &mut Cache,
) {
    supply::process_deposit_addresses(tx, queues, &mut cache.supply);
}

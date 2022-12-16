/// Handle explicit CEX address id's
use super::Cache;
use postgres::Transaction;

/*
    In schema data, define addresses as text in dedicated table.
    In master table, use address id's.
    At each block inclusion, check if new main address got added to core.addresses
    and add its id to master table.
    Cache status whether any main addresses not yet in core.addresses.
*/

/// Find out if there are any predefined main addresses without an id.
pub(super) fn any_unseen_main_addresses(tx: &mut Transaction) -> bool {
    let row = tx
        .query_one(
            "
            select exists(
                select lst.address
                from cex.main_addresses_list lst
                left join core.addresses adr on adr.address = lst.address
                where adr.id is null
            );",
            &[],
        )
        .unwrap();
    row.get(0)
}

/// Find out if there are any predefined ignored addresses without an id.
pub(super) fn any_unseen_ignored_addresses(tx: &mut Transaction) -> bool {
    let row = tx
        .query_one(
            "
            select exists(
                select lst.address
                from cex.ignored_addresses_list lst
                left join core.addresses adr on adr.address = lst.address
                where adr.id is null
            );",
            &[],
        )
        .unwrap();
    row.get(0)
}

/// Check for unseen main addresses in current block.
pub(super) fn declare_main_addresses(tx: &mut Transaction, cache: &mut Cache, height: i32) {
    let n_mod = tx
        .execute(
            "
            insert into cex.main_addresses(address_id, cex_id)
                select adr.id
                    , lst.cex_id
                from cex.main_addresses_list lst
                -- TODO: check if joining on md5 is faster
                join core.addresses adr on adr.address = lst.address
                left join cex.main_addresses mas on mas.address_id = adr.id
                where adr.spot_height = $1
                    and mas.address_id is null;
        ",
            &[&height],
        )
        .unwrap();
    if n_mod > 0 {
        cache.unseen_main_addresses = any_unseen_main_addresses(tx);
    }
}

/// Check for unseen ignored addresses in current block.
pub(super) fn declare_ignored_addresses(tx: &mut Transaction, cache: &mut Cache, height: i32) {
    let n_mod = tx
        .execute(
            "
            insert into cex.deposit_addresses_ignored(address_id)
                select adr.id
                from cex.ignored_addresses_list lst
                join core.addresses adr on adr.address = lst.address
                left join cex.deposit_addresses_ignored ign on ign.address_id = adr.id
                where adr.spot_height = $1
                    and ign.address_id is null;
        ",
            &[&height],
        )
        .unwrap();
    if n_mod > 0 {
        cache.unseen_ignored_addresses = any_unseen_ignored_addresses(tx);
    }
}

/// Undo main/ignored addresses declared in current block
pub(super) fn rollback_address_declarations(tx: &mut Transaction, cache: &mut Cache, height: i32) {
    // Main addresses
    tx.execute(
        "
        delete from cex.main_addresses cas
        using core.addresses adr
        where adr.id = cas.address_id
            and adr.spot_height = $1;
        ",
        &[&height],
    )
    .unwrap();
    cache.unseen_main_addresses = any_unseen_main_addresses(tx);

    // Ignored addresses
    tx.execute(
        "
        delete from cex.deposit_addresses_ignored ign
        using core.addresses adr
        where adr.id = ign.address_id
            and adr.spot_height = $1;
        ",
        &[&height],
    )
    .unwrap();
    cache.unseen_ignored_addresses = any_unseen_ignored_addresses(tx);
}

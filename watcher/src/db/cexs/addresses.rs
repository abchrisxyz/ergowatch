use super::processing_log;
use super::Cache;
use crate::parsing::BlockData;
use log::info;
use postgres::Transaction;

/*
    In schema data, define addresses as text in dedicated table.
    In master table, use address id's.
    At each block inclusion, check if new main address got added to core.addresses
    and add its id to master table.
    Cache status whether any main addresses not yet in core.addresses.
*/

/// Find new deposit addresses
///
/// Returns height of earliest tx involving newly found addresses.
pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) -> Option<i32> {
    if cache.unseen_main_addresses {
        declare_main_addresses(tx, cache, block.height);
    }
    if cache.unseen_ignored_addresses {
        declare_ignored_addresses(tx, cache, block.height);
    }
    spot_deposit_candidates(tx, block.height);
    insert_new_deposit_addresses(tx, block.height)
}

/// Remove deposit addresses spotted in block
pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    rollback_conflict_resolution_at_height(tx, block.height);
    delete_deposit_addresses_at_height(tx, block.height);
    rollback_address_declarations(tx, cache, block.height);
}

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
                left join core.addresses adr on adr.address =lst.address
                where adr.id is null
            );",
            &[],
        )
        .unwrap();
    row.get(0)
}

/// Check for unseen main addresses in current block.
fn declare_main_addresses(tx: &mut Transaction, cache: &mut Cache, height: i32) {
    let n_mod = tx
        .execute(
            "
            insert into cex.addresses(address_id, cex_id, type)
                select adr.id
                    , lst.cex_id
                    , 'main'
                from cex.main_addresses_list lst
                -- TODO: check if joining on md5 is faster
                join core.addresses adr on adr.address = lst.address
                left join cex.addresses cas on cas.address_id = adr.id
                where adr.spot_height = $1
                    and cas.address_id is null;
        ",
            &[&height],
        )
        .unwrap();
    if n_mod > 0 {
        cache.unseen_main_addresses = any_unseen_main_addresses(tx);
    }
}

/// Check for unseen ignored addresses in current block.
fn declare_ignored_addresses(tx: &mut Transaction, cache: &mut Cache, height: i32) {
    let n_mod = tx
        .execute(
            "
            insert into cex.addresses_ignored(address_id)
                select adr.id
                from cex.main_addresses_list lst
                -- TODO: check if joining on md5 is faster
                join core.addresses adr on adr.address = lst.address
                left join cex.addresses_ignored ign on ign.address_id = adr.id
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
fn rollback_address_declarations(tx: &mut Transaction, cache: &mut Cache, height: i32) {
    // Main addresses
    tx.execute(
        "
        delete from cex.addresses cas
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
        delete from cex.addresses_ignored ign
        using core.addresses adr
        where adr.id = ign.address_id
            and adr.spot_height = $1;
        ",
        &[&height],
    )
    .unwrap();
    cache.unseen_ignored_addresses = any_unseen_ignored_addresses(tx);
}

/// Creates a temp table holding new deposit address candidates
/// spotted at given `height`.
fn spot_deposit_candidates(tx: &mut Transaction, height: i32) {
    tx.execute(
        "
        create temp table _cex_deposit_candidates as
            with to_main_txs as ( 
                select cas.cex_id
                    , dif.tx_id
                    , dif.value
                    , cas.address_id as main_address_id
                from cex.addresses cas
                join adr.erg_diffs dif on dif.address_id = cas.address_id
                where cas.type = 'main'
                    and dif.height = $1
                    and dif.value > 0
            )
            select dif.address_id
                , txs.cex_id 
            from adr.erg_diffs dif
            join to_main_txs txs on txs.tx_id = dif.tx_id
            -- be aware of known addresses for each cex
            left join cex.addresses cas
                on cas.address_id = dif.address_id
                and cas.cex_id = txs.cex_id
            left join cex.addresses_conflicts con
                on con.address_id = dif.address_id
            -- ignored addresses
            left join cex.addresses_ignored ign 
                on ign.address_id = dif.address_id
            -- full address
            join core.addresses adr
                on adr.id = dif.address_id
            where dif.value < 0
                and dif.height = $1
                -- exclude txs from known cex addresses
                and cas.address_id is null
                and con.address_id is null
                -- exclude contract addresses
                and starts_with(adr.address, '9')
                and length(adr.address) = 51
                -- exclude ignored addresses
                and ign.address_id is null
            -- dissolve duplicates from multiple txs in same block
            group by 1, 2;
        ",
        &[&height],
    )
    .unwrap();
}

/// Store new deposit addresses in cex.addresses
///
/// Returns height of earliest tx involving spotted addresses.
fn insert_new_deposit_addresses(tx: &mut Transaction, height: i32) -> Option<i32> {
    // Handle any upcoming conflicts
    if let Some(conflicting_addresses) = get_cex_conflict_addresses(tx) {
        for address_id in conflicting_addresses {
            info!(
                "Found existing deposit address linked to multiple CEX's: {}",
                address_id
            );
            log_cex_conflict(tx, address_id, height);
            update_processing_log_for_conflict(tx, address_id);
            resolve_cex_conflict(tx, address_id);
        }
    }
    if let Some(airdrop_addresses) = get_cex_airdrop_addresses(tx, height) {
        for aa in airdrop_addresses {
            info!(
                "Found candidate deposit address linked to multiple CEX's: {}",
                aa.address_id
            );
            log_cex_airdrop_conflict(tx, &aa);
            resolve_cex_conflict(tx, aa.address_id);
        }
    }
    tx.execute(
        "
        insert into cex.addresses (address_id, cex_id, type, spot_height)
        select address_id
            , cex_id 
            , 'deposit'
            , $1
        from _cex_deposit_candidates;",
        &[&height],
    )
    .unwrap();

    // Retrun earliest tx height involving one of the new addresses
    tx.query_one(
        "
        select min(dif.height)
        from adr.erg_diffs dif
        join _cex_deposit_candidates can
            on can.address_id = dif.address_id;",
        &[],
    )
    .unwrap()
    .get(0)
}

/// Returns candidates already linked to a different CEX
fn get_cex_conflict_addresses(tx: &mut Transaction) -> Option<Vec<i64>> {
    tx.query_one(
        "
        select array_agg(a.address_id)
        from _cex_deposit_candidates c
        join cex.addresses a
            on a.address_id = c.address_id
            and a.cex_id <> c.cex_id;
        ",
        &[],
    )
    .unwrap()
    .get(0)
}

/// Add address in cex.addresses_conflicts
fn log_cex_conflict(tx: &mut Transaction, address_id: i64, conflict_height: i32) {
    tx.execute(
        "
        insert into cex.addresses_conflicts (
            address_id,
            first_cex_id,
            type,
            spot_height,
            conflict_spot_height
        )
        select cas.address_id
            , cas.cex_id
            , cas.type
            , cas.spot_height
            , $2
        from cex.addresses cas
        where cas.address_id = $1
        ",
        &[&address_id, &conflict_height],
    )
    .unwrap();
}

/// Removes address from cex.addresses and temp work table
fn resolve_cex_conflict(tx: &mut Transaction, address_id: i64) {
    tx.execute(
        "delete from cex.addresses where address_id = $1;",
        &[&address_id],
    )
    .unwrap();
    tx.execute(
        "delete from _cex_deposit_candidates where address_id = $1;",
        &[&address_id],
    )
    .unwrap();
}

/// Addresses sending to multiple CEX's at once.
///
/// Often resulting from token airdrops sending to top x ERG holding addresses.
struct AirdropAddress {
    address_id: i64,
    first_cex_id: i32,
    spot_height: i32,
}

/// Retrieves airdop addresses from candidates
///
/// `height`: current block height
fn get_cex_airdrop_addresses(tx: &mut Transaction, height: i32) -> Option<Vec<AirdropAddress>> {
    let aas: Vec<AirdropAddress> = tx
        .query(
            "
        select address_id
            , min(cex_id) as first_cex_id
        from _cex_deposit_candidates
        where address_id in (
            select address_id
            from _cex_deposit_candidates
            group by 1 having count(*) > 1
        )
        group by 1
        ",
            &[],
        )
        .unwrap()
        .iter()
        .map(|row| AirdropAddress {
            address_id: row.get(0),
            first_cex_id: row.get(1),
            spot_height: height,
        })
        .collect();

    match aas.is_empty() {
        true => None,
        false => Some(aas),
    }
}

/// Add address in cex.addresses_conflicts
fn log_cex_airdrop_conflict(tx: &mut Transaction, aa: &AirdropAddress) {
    tx.execute(
        "
        insert into cex.addresses_conflicts (
            address_id,
            first_cex_id,
            type,
            spot_height,
            conflict_spot_height
        ) values ($1, $2, 'deposit', $3, $3);
        ",
        &[&aa.address_id, &aa.first_cex_id, &aa.spot_height],
    )
    .unwrap();
}

/// Delete deposit address spotted at given height
fn delete_deposit_addresses_at_height(tx: &mut Transaction, height: i32) {
    // Explicitly exclude main addresses, even though they have no spot_height
    tx.execute(
        "delete from cex.addresses where spot_height = $1 and type <> 'main';",
        &[&height],
    )
    .unwrap();
}

fn rollback_conflict_resolution_at_height(tx: &mut Transaction, height: i32) {
    // Restore addresses in cex.addresses
    tx.execute(
        "
        insert into cex.addresses (address_id, cex_id, type, spot_height)
        select address_id
            , first_cex_id 
            , type
            , spot_height
        from cex.addresses_conflicts
        where conflict_spot_height = $1; 
        ",
        &[&height],
    )
    .unwrap();

    // Update processing log
    let spot_heights: Option<Vec<i32>> = tx
        .query_one(
            "
        select array_agg(distinct spot_height)
        from cex.addresses_conflicts
        where conflict_spot_height = $1;
        ",
            &[&height],
        )
        .unwrap()
        .get(0);
    if let Some(heights) = spot_heights {
        for spot_height in heights {
            rollback_update_processing_log_for_conflict(tx, spot_height);
        }
    }

    // Remove from conflict log
    tx.execute(
        "
        delete from cex.addresses_conflicts
        where conflict_spot_height = $1; 
        ",
        &[&height],
    )
    .unwrap();
}

/// Find out what the invalidation height of the block where the conflicting
/// address was first spotted and adjust processing log accordingly.
fn update_processing_log_for_conflict(tx: &mut Transaction, address_id: i64) {
    let spot_height: i32 = tx
        .query_one(
            "select spot_height from cex.addresses where address_id = $1;",
            &[&address_id],
        )
        .unwrap()
        .get(0);
    let header_id: String = tx
        .query_one(
            "select id from core.headers where height = $1;",
            &[&spot_height],
        )
        .unwrap()
        .get(0);
    let new_invalidation_height: Option<i32> = tx
        .query_one(
            "
            with other_addresses_spotted_at_same_height as (
                select address_id
                from cex.addresses
                where spot_height = $1
                    and address_id <> $2
                    and type = 'deposit'
            )
            select min(dif.height)
            from adr.erg_diffs dif
            join other_addresses_spotted_at_same_height ads
                on ads.address_id = dif.address_id;
            ",
            &[&spot_height, &address_id],
        )
        .unwrap()
        .get(0);
    processing_log::update_invalidation_height(tx, &header_id, new_invalidation_height);
}

/// Restore original invalidation height of previously corrected log entry.
fn rollback_update_processing_log_for_conflict(tx: &mut Transaction, spot_height: i32) {
    let header_id: String = tx
        .query_one(
            "select id from core.headers where height = $1;",
            &[&spot_height],
        )
        .unwrap()
        .get(0);
    let new_invalidation_height: Option<i32> = tx
        .query_one(
            "
            with addresses_spotted_at_height as (
                select address_id
                from cex.addresses
                where spot_height = $1
            )
            select min(dif.height)
            from adr.erg_diffs dif
            join addresses_spotted_at_height ads
                on ads.address_id = dif.address_id;
            ",
            &[&spot_height],
        )
        .unwrap()
        .get(0);
    processing_log::update_invalidation_height(tx, &header_id, new_invalidation_height);
}

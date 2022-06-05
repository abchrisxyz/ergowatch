use super::processing_log;
use crate::parsing::BlockData;
use log::info;
use postgres::Transaction;

/// Find new deposit addresses
///
/// Returns height of earliest tx involving newly found addresses.
pub(super) fn include(tx: &mut Transaction, block: &BlockData) -> Option<i32> {
    spot_deposit_candidates(tx, block.height);
    insert_new_deposit_addresses(tx, block.height)
}

/// Remove deposit addresses spotted in block
pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    rollback_conflict_resolution_at_height(tx, block.height);
    delete_addresses_at_height(tx, block.height);
}

/// Creates a temp table holding new deposit address candidates
/// spotted at given height.
fn spot_deposit_candidates(tx: &mut Transaction, height: i32) {
    tx.execute(
        "
        create temp table _cex_deposit_candidates as
            with to_main_txs as ( 
                select cas.cex_id
                    , dif.tx_id
                    , dif.value
                    , cas.address as main_address
                from cex.addresses cas
                join bal.erg_diffs dif on dif.address = cas.address
                where cas.type = 'main'
                    and dif.height = $1
                    and dif.value > 0
            )
            select dif.address
                , txs.cex_id 
            from bal.erg_diffs dif
            join to_main_txs txs on txs.tx_id = dif.tx_id
            -- be aware of known addresses for each cex
            left join cex.addresses cas
                on cas.address = dif.address
                and cas.cex_id = txs.cex_id
            left join cex.addresses_conflicts con
                on con.address = dif.address
            -- ignored addresses
            left join cex.addresses_ignored ign 
                on ign.address = dif.address
            where dif.value < 0
                and dif.height = $1
                -- exclude txs from known cex addresses
                and cas.address is null
                and con.address is null
                -- exclude contract addresses
                and starts_with(dif.address, '9')
                and length(dif.address) = 51
                -- exclude ignored addresses
                and ign.address is null
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
        for address in conflicting_addresses {
            info!(
                "Found deposit address linked to multiple CEX's: {}",
                address
            );
            log_cex_conflict(tx, &address, height);
            update_processing_log_for_conflict(tx, &address);
            resolve_cex_conflict(tx, &address);
        }
    }
    tx.execute(
        "
        insert into cex.addresses (address, cex_id, type, spot_height)
        select address
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
        from bal.erg_diffs dif
        join _cex_deposit_candidates can
            on can.address = dif.address;",
        &[],
    )
    .unwrap()
    .get(0)
}

/// Returns candidates already linked to a different CEX
fn get_cex_conflict_addresses(tx: &mut Transaction) -> Option<Vec<String>> {
    tx.query_one(
        "
        select array_agg(distinct a.address)
        from _cex_deposit_candidates c
        join cex.addresses a
            on a.address = c.address
            and a.cex_id <> c.cex_id;
        ",
        &[],
    )
    .unwrap()
    .get(0)
}

/// Add address in cex.addresses_conflicts
fn log_cex_conflict(tx: &mut Transaction, address: &String, conflict_height: i32) {
    tx.execute(
        "
        insert into cex.addresses_conflicts (
            address,
            first_cex_id,
            type,
            spot_height,
            conflict_spot_height
        )
        select cas.address
            , cas.cex_id
            , cas.type
            , cas.spot_height
            , $2
        from cex.addresses cas
        where cas.address = $1
        ",
        &[&address, &conflict_height],
    )
    .unwrap();
}

/// Removes address from cex.addresses and temp work table
fn resolve_cex_conflict(tx: &mut Transaction, address: &String) {
    tx.execute("delete from cex.addresses where address = $1;", &[&address])
        .unwrap();
    tx.execute(
        "delete from _cex_deposit_candidates where address = $1;",
        &[&address],
    )
    .unwrap();
}

/// Delete address spotted at given height
fn delete_addresses_at_height(tx: &mut Transaction, height: i32) {
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
        insert into cex.addresses (address, cex_id, type, spot_height)
        select address
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
fn update_processing_log_for_conflict(tx: &mut Transaction, address: &str) {
    let spot_height: i32 = tx
        .query_one(
            "select spot_height from cex.addresses where address = $1;",
            &[&address],
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
                select address
                from cex.addresses
                where spot_height = $1
                    and address <> $2
            )
            select min(dif.height)
            from bal.erg_diffs dif
            join other_addresses_spotted_at_same_height ads
                on ads.address = dif.address;
            ",
            &[&spot_height, &address],
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
                select address
                from cex.addresses
                where spot_height = $1
            )
            select min(dif.height)
            from bal.erg_diffs dif
            join addresses_spotted_at_height ads
                on ads.address = dif.address;
            ",
            &[&spot_height],
        )
        .unwrap()
        .get(0);
    processing_log::update_invalidation_height(tx, &header_id, new_invalidation_height);
}

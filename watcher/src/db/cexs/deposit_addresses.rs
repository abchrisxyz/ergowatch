/// Deposit address detection
use crate::parsing::BlockData;
use log::info;
use postgres::Client;
use postgres::Transaction;

// A CEX deposit addresses (DA) is defined as an addresses sending at least
// once to one or more main addresses of a single CEX.
// Some addresses will send to more than one CEX (airdrops) and need to be
// ignored. This can happen in a single block or over multiple ones, in which
// case the address might first appear as a legitimate DA.
//
// There are five basic actions that can be performed for a given address:
// - Register: record address as being a deposit address
// - Unregister: remove address from list of known deposit addresses
// - Exclude: record address to never be considered as a deposit address
// - Propagate: reflect registration in depending tables
// - Purge: reflect unregistration in depending tables
//
// To ensure consistency between cex.deposit_addresses and dependent tables,
// all actions changing either one of those (all except exlusion) need to be
// performed in a single db transaction.
// Propagation can be time consuming. To allow delaying and/or batching of
// propagation/purgation actions, actions are first staged for execution.
// Each action has a dedicated table that serves as a queue in wich DA's
// are listed for future processing.
// Only exclusion is performed straight away (with each block) and has no
// queue.
//
// When parsing a new block, three things can happen:
// A. a new deposit address is discovered
// B. a known deposit address is found sending to a different CEX
// C. an unknown address is found sending to multiple CEXs in the same block
//
// The ruleset bellow shows which actions to perform for each scenario.
//
// A. On new DA detection:
//  - stage address for registration
//  - stage address for propagation
//
// B. Existing DA linked to different CEX:
//  - if already processed
//      - stage address for unregistration
//      - stage address for purgation
//      - add address to exclusion table
//  - else:
//      - remove from registration queue
//
// C. New address sending to multiple CEXs
//  - add address to exclusion table
//
// Lastly, there are block rollbacks in the event of a fork.
// Staging a rollback for delayed processing gets complex as the balance data
// of an address could be gone or altered when the rollback is applied.
// Instead, rollbacks are applied on the spot. Addresses to be propagated or
// purged are returned to caller to be processed by relevant modules.
// Ruleset:
//  - if block height yet to be processed:
//      - remove addresses from registration and propagation queues if staged
//        in that block (revert scenario A)
//      - revert scenario B:
//          - remove addresses from unregistration and purgation queue
//          - remove from exclusion table if conflict spotted in that block and
//            address spotted in earlier block
//      - delete any other addresses from cex.deposit_addresses_excluded if
//        conflict spotted in that block (revert scenarios C)
//  - if block height already processed:
//      - unregister and purge addresses that are in cex.deposit_addresses and
//        were spotted in that block (revert scenario A)
//      - register and propagate addresses in cex.deposit_addresses_excluded if
//        conflict spotted in that block and address spotted in earlier block,
//        then delete them from exlusion table (revert scenario B)
//      - decrement height of processed height tracker
//      - delete any other addresses from cex.deposit_addresses_excluded if
//        conflict spotted in that block (revert scenarios C)

/// Addresses that need to be propagated or purged
pub struct AddressQueues {
    pub propagate: Vec<i64>,
    pub purge: Vec<i64>,
}

/// Detect new deposit address changes in unprocessed blocks up to `max_height`
pub fn spot(tx: &mut Transaction, max_height: i32, cache: &Cache) -> AddressQueues {
    let min_height = cache.last_processed_height + 1;
    spot_range(tx, min_height, max_height)
}

pub(super) fn spot_range(tx: &mut Transaction, min_height: i32, max_height: i32) -> AddressQueues {
    info!(
        "Searching for new deposit addresses in blocks {} - {}",
        min_height, max_height
    );
    candidates::spot(tx, min_height, max_height);

    // Handle any upcoming conflicts
    let false_positives = candidates::get_false_positives(tx);
    for conflict in &false_positives {
        processing::exclude(tx, &conflict);
        processing::unregister_address(tx, conflict.address_id);
        candidates::delete_address(tx, conflict.address_id);
    }
    for conflict in candidates::get_conflicts(tx) {
        processing::exclude(tx, &conflict);
        candidates::delete_address(tx, conflict.address_id);
    }
    // Remaining candidates are all valid
    processing::register_candidates(tx);
    let new_addresses = candidates::get_all(tx);

    // Update processing height
    set_last_processed_height(tx, max_height);

    // Drop temp table
    candidates::drop_work_table(tx);

    // Return queues
    AddressQueues {
        propagate: new_addresses,
        purge: false_positives.iter().map(|c| c.address_id).collect(),
    }
}

/// Undo block
pub(super) fn rollback(
    tx: &mut Transaction,
    block: &BlockData,
    cache: &mut Cache,
) -> Option<AddressQueues> {
    let height = block.height;
    assert!(height >= cache.last_processed_height);
    if height > cache.last_processed_height {
        None
    } else {
        let deposit_addresses = rollback::get_addresses_spotted_at(tx, height);
        for address_id in &deposit_addresses {
            processing::unregister_address(tx, *address_id);
        }

        let conflicts = rollback::get_conflicts_spotted_at(tx, height);
        for conflict in &conflicts {
            if conflict.address_spot_height < height {
                rollback::reregister_conflict(tx, conflict);
            }
            rollback::unexclude(tx, conflict);
        }

        // Decrement processing height
        decrement_last_processed_height(tx);
        cache.last_processed_height -= 1;

        Some(AddressQueues {
            propagate: conflicts
                .iter()
                .filter(|c| c.address_spot_height != height)
                .map(|c| c.address_id)
                .collect(),
            purge: deposit_addresses,
        })
    }
}

struct Conflict {
    address_id: i64,
    address_spot_height: i32,
    conflict_spot_height: i32,
}

/// Manage temporary deposit candidates table
mod candidates {
    use super::Conflict;
    use postgres::Transaction;

    /// Creates a temp table holding new deposit address candidates.
    pub(super) fn spot(tx: &mut Transaction, min_height: i32, max_height: i32) {
        tx.execute(
            "
            create unlogged table _cex_deposit_candidates as
            with to_main_txs as ( 
                select mas.cex_id
                    , dif.height
                    , dif.tx_id
                    , dif.value
                from cex.main_addresses mas
                join adr.erg_diffs dif on dif.address_id = mas.address_id
                where dif.height >= $1
                    and dif.height <= $2
                    and dif.value > 0
            )
            select dif.address_id
                , txs.cex_id
                , min(txs.height) as spot_height 
            from adr.erg_diffs dif
            join to_main_txs txs on txs.tx_id = dif.tx_id
            -- be aware of known addresses for each cex
            left join cex.main_addresses mas
                on mas.address_id = dif.address_id
            left join cex.deposit_addresses das
                on das.address_id = dif.address_id
                and das.cex_id = txs.cex_id
            -- already excluded addresses
            left join cex.deposit_addresses_excluded dax
                on dax.address_id = dif.address_id
            -- ignored addresses
            left join cex.deposit_addresses_ignored ign 
                on ign.address_id = dif.address_id
            -- full address
            join core.addresses adr
                on adr.id = dif.address_id
            where dif.value < 0
                and dif.height >= $1
                and dif.height <= $2
                -- exclude txs from known cex addresses
                and mas.address_id is null
                and das.address_id is null
                and dax.address_id is null
                -- exclude contract addresses
                -- exclude contract addresses
                and adr.p2pk
                -- exclude ignored addresses
                and ign.address_id is null
            -- dissolve duplicates
            group by 1, 2;
        ",
            &[&min_height, &max_height],
        )
        .unwrap();
    }

    /// Delete given address from candidate table
    pub(super) fn delete_address(tx: &mut Transaction, address_id: i64) {
        tx.execute(
            "delete from _cex_deposit_candidates where address_id = $1;",
            &[&address_id],
        )
        .unwrap();
    }

    /// Spot false positives among deposit candidates
    pub(super) fn get_false_positives(tx: &mut Transaction) -> Vec<Conflict> {
        tx.query(
            "
            select a.address_id
                , a.spot_height
                , c.spot_height                    
            from _cex_deposit_candidates c
            join cex.deposit_addresses a
                on a.address_id = c.address_id
                and a.cex_id <> c.cex_id;
            ",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| Conflict {
            address_id: r.get(0),
            address_spot_height: r.get(1),
            conflict_spot_height: r.get(2),
        })
        .collect()
    }

    /// Retrieves airdop addresses from candidates
    pub(super) fn get_conflicts(tx: &mut Transaction) -> Vec<Conflict> {
        tx.query(
            "
            select address_id
                , min(spot_height) as address_spot_height
                , (array_agg(spot_height order by spot_height))[2] as conflict_spot_height
            from _cex_deposit_candidates
            group by 1 having count(*) > 1
            
            ",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| Conflict {
            address_id: r.get(0),
            address_spot_height: r.get(1),
            conflict_spot_height: r.get(2),
        })
        .collect()
    }

    /// Get all address id's in candidate table
    pub(super) fn get_all(tx: &mut Transaction) -> Vec<i64> {
        tx.query(
            "
            select address_id
            from _cex_deposit_candidates;
            ",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
    }

    /// Cleanup work tables
    pub(super) fn drop_work_table(tx: &mut Transaction) {
        tx.execute("drop table _cex_deposit_candidates;", &[])
            .unwrap();
    }
}

mod processing {
    use super::Conflict;
    use postgres::Transaction;

    pub(super) fn register_candidates(tx: &mut Transaction) {
        tx.execute(
            "
            insert into cex.deposit_addresses (
                address_id,
                cex_id,
                spot_height
            )
            select address_id
                , cex_id
                , spot_height
            from _cex_deposit_candidates;
            ",
            &[],
        )
        .unwrap();
    }

    pub(super) fn unregister_address(tx: &mut Transaction, address_id: i64) {
        tx.execute(
            "
            delete from cex.deposit_addresses
            where address_id = $1;
            ",
            &[&address_id],
        )
        .unwrap();
    }

    /// Add to exclusion table
    pub(super) fn exclude(tx: &mut Transaction, c: &Conflict) {
        tx.execute(
            "
            insert into cex.deposit_addresses_excluded (
                address_id,
                address_spot_height,
                conflict_spot_height
            )
            values($1, $2, $3);
            ",
            &[
                &c.address_id,
                &c.address_spot_height,
                &c.conflict_spot_height,
            ],
        )
        .unwrap();
    }
}

mod rollback {
    use super::Conflict;
    use postgres::Transaction;

    /// Collect addresses spotted at given `height`
    pub(super) fn get_addresses_spotted_at(tx: &mut Transaction, height: i32) -> Vec<i64> {
        tx.query(
            "
            select address_id
            from cex.deposit_addresses
            where spot_height = $1;
            ",
            &[&height],
        )
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
    }

    /// Collect conflicts spotted at given `height`
    pub(super) fn get_conflicts_spotted_at(tx: &mut Transaction, height: i32) -> Vec<Conflict> {
        tx.query(
            "
            select address_id
                , address_spot_height
                , conflict_spot_height
            from cex.deposit_addresses_excluded
            where conflict_spot_height = $1;
            ",
            &[&height],
        )
        .unwrap()
        .iter()
        .map(|r| Conflict {
            address_id: r.get(0),
            address_spot_height: r.get(1),
            conflict_spot_height: r.get(2),
        })
        .collect()
    }

    /// Insert conflict back into cex.deposit_addresses
    pub(super) fn reregister_conflict(tx: &mut Transaction, c: &Conflict) {
        let cex_id: i32 = tx
            .query_one(
                "
                with from_address_txs as ( 
                    select height
                        , tx_id
                    from adr.erg_diffs
                    where address_id = $1
                        and value < 0
                        and height < $2
                )
                select distinct m.cex_id
                from adr.erg_diffs d
                join from_address_txs t
                    on t.height = d.height
                    and t.tx_id = d.tx_id
                join cex.main_addresses m
                    on m.address_id = d.address_id
                where d.value > 0;
            ",
                &[&c.address_id, &c.conflict_spot_height],
            )
            .unwrap()
            .get(0);

        tx.execute(
            "
            insert into cex.deposit_addresses (
                address_id,
                cex_id,
                spot_height
            )
            values ($1, $2, $3);",
            &[&c.address_id, &cex_id, &c.address_spot_height],
        )
        .unwrap();
    }

    /// Remove from exclusion table
    pub(super) fn unexclude(tx: &mut Transaction, c: &Conflict) {
        tx.execute(
            "
            delete from cex.deposit_addresses_excluded
            where address_id = $1;
            ",
            &[&c.address_id],
        )
        .unwrap();
    }
}

#[derive(Debug)]
pub struct Cache {
    pub last_processed_height: i32,
}

impl Cache {
    pub(super) fn new() -> Self {
        Self {
            last_processed_height: 0,
        }
    }

    pub(super) fn load(client: &mut Client) -> Self {
        Self {
            last_processed_height: get_last_processed_height(client),
        }
    }
}

fn get_last_processed_height(client: &mut Client) -> i32 {
    client
        .query_one(
            "select last_processed_height from cex._deposit_addresses_log;",
            &[],
        )
        .unwrap()
        .get(0)
}

fn set_last_processed_height(tx: &mut Transaction, height: i32) {
    tx.execute(
        "
        update cex._deposit_addresses_log
        set last_processed_height = $1;
        ",
        &[&height],
    )
    .unwrap();
}

fn decrement_last_processed_height(tx: &mut Transaction) {
    tx.execute(
        "
        update cex._deposit_addresses_log
        set last_processed_height = last_processed_height - 1;
        ",
        &[],
    )
    .unwrap();
}

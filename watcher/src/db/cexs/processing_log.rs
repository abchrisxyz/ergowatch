use crate::parsing::BlockData;
use postgres::Transaction;

pub(super) enum Status {
    Pending,
    PendingRollback,
    Processed,
    ProcessedRollback,
}

/// Add new block with `pending` status
pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    insert_pending_block(tx, block.header_id, block.height);
}

/// Remove block from log if still `pending` or mark for rollback
/// during next repair event.
pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    let status = get_block_status(tx, block.header_id);
    match status {
        Status::Pending => {
            tx.execute(DELETE_PENDING_BLOCK, &[&block.header_id])
                .unwrap();
        }
        Status::Processed => {
            // Mark processed block as pending_rollback
            set_block_status(tx, &block.header_id, Status::PendingRollback);
        }
        Status::PendingRollback | Status::ProcessedRollback => {
            // Not supposed to happen
            panic!("A block cannot be rolled back more than once.")
        }
    }
}

/// Processing status of new deposit addresses for given block
fn get_block_status(tx: &mut Transaction, header_id: &str) -> Status {
    let qry = "
        select status::text
        from cex.block_processing_log
        where header_id = $1;
    ";
    let row = tx.query_one(qry, &[&header_id]).unwrap();
    let status: &str = row.get("status");
    match status {
        "pending" => Status::Pending,
        "pending_rollback" => Status::PendingRollback,
        "processed" => Status::Processed,
        "processed_rollback" => Status::ProcessedRollback,
        _ => panic!("Unknown block status value for CEX deposit addresses processing log"),
    }
}

fn set_block_status(tx: &mut Transaction, header_id: &str, status: Status) {
    let status_text = match status {
        Status::Pending => "pending",
        Status::Processed => "processed",
        Status::PendingRollback => "pending_rollback",
        Status::ProcessedRollback => "processed_rollback",
    };
    tx.execute(
        "
        update cex.block_processing_log
        set status = $2
        where header_id = $1;",
        &[&header_id, &status_text],
    )
    .unwrap();
}

/// Add block with `pending` status
fn insert_pending_block(tx: &mut Transaction, header_id: &str, height: i32) {
    tx.execute(
        "
        with to_main_txs as ( 
            select cas.cex_id
                , dif.tx_id
                , dif.value
                , cas.address as main_address
            from cex.addresses cas
            join bal.erg_diffs dif on dif.address = cas.address
            where cas.type = 'main'
                and dif.height = $2
                and dif.value > 0
        ), new_deposit_addresses as (
            select distinct dif.address
            from bal.erg_diffs dif
            join to_main_txs txs on txs.tx_id = dif.tx_id
            -- be aware of known addresses
            left join cex.addresses cas
                on cas.address = dif.address
                and cas.cex_id = txs.cex_id
            where dif.value < 0
                and dif.height = $2
                -- exclude txs from known cex addresses
                and cas.address is null
        )
        insert into cex.block_processing_log (
            header_id,
            height,
            invalidation_height,
            status
        )
        select $1
            , $2 
            , min(dif.height)
            , 'pending'
        from new_deposit_addresses dep
        join bal.erg_diffs dif on dif.address = dep.address;
        ",
        &[&header_id, &height],
    )
    .unwrap();
}

// Rollback

/// Removes block from log if still pending.
///
/// Noop for any other block status.
pub const DELETE_PENDING_BLOCK: &str = "
    delete from cex.block_processing_log
    where header_id = $1 and status = 'pending';";

pub mod repair {
    use postgres::Transaction;

    /// Mark blocks at given height as processed
    pub fn set_height_pending_to_processed(tx: &mut Transaction, height: i32) {
        tx.execute(
            "
            update cex.block_processing_log
            set status = 'processed'
            where status = 'pending'
                and height =  $1;
        ",
            &[&height],
        )
        .unwrap();

        tx.execute(
            "
            update cex.block_processing_log
            set status = 'processed_rollback'
            where status = 'pending_rollback'
                and height =  $1;
        ",
            &[&height],
        )
        .unwrap();
    }

    /// Mark blocks without an invalidation height as processed
    pub fn set_non_invalidating_blocks_to_processed(tx: &mut Transaction) {
        tx.execute(
            "
            update cex.block_processing_log
            set status = 'processed'
            where status = 'pending'
                and invalidation_height is null;
        ",
            &[],
        )
        .unwrap();

        tx.execute(
            "
            update cex.block_processing_log
            set status = 'processed_rollback'
            where status = 'pending_rollback'
                and invalidation_height is null;
        ",
            &[],
        )
        .unwrap();
    }
}

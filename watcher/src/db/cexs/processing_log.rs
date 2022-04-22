use crate::parsing::BlockData;
use postgres::Transaction;

pub(super) enum Status {
    Pending,
    PendingRollback,
    Processing,
    ProcessingRollback,
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
        Status::Processing | Status::Processed => {
            // Mark processed block as pending_rollback
            // Mark processing block as pending_rollback
            set_block_status(tx, &block.header_id, Status::PendingRollback);
        }
        Status::PendingRollback | Status::ProcessingRollback | Status::ProcessedRollback => {
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
        "processing" => Status::Processing,
        "processing_rollback" => Status::ProcessingRollback,
        "processed" => Status::Processed,
        "processed_rollback" => Status::ProcessedRollback,
        _ => panic!("Unknown block status value for CEX deposit addresses processing log"),
    }
}

fn set_block_status(tx: &mut Transaction, header_id: &str, status: Status) {
    let status_text = match status {
        Status::Pending => "pending",
        Status::Processing => "processing",
        Status::Processed => "processed",
        Status::PendingRollback => "pending_rollback",
        Status::ProcessingRollback => "processing_rollback",
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
        insert into cex.block_processing_log (
            header_id,
            height,
            invalidation_height,
            status
        )
        select $1
            , $2
            , min(dif.height) as invalidation_height
            , 'pending'
        from cex.new_deposit_addresses dep
        join bal.erg_diffs dif on dif.address = dep.address
        where dep.spot_height = $2;",
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

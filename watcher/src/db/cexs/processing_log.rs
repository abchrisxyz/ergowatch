use crate::parsing::BlockData;
use postgres::Transaction;

pub(super) enum Status {
    Pending,
    PendingRollback,
    Processed,
    ProcessedRollback,
}

/// Add new block with `pending` status
pub(super) fn include(tx: &mut Transaction, block: &BlockData, invalidation_height: Option<i32>) {
    insert_pending_block(tx, block.header_id, block.height, invalidation_height);
}

/// Remove block from log if still `pending` or mark for rollback
/// during next repair event.
pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    let status = get_block_status(tx, block.header_id);
    match status {
        Status::Pending => delete_pending_block(tx, &block.header_id),
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
fn insert_pending_block(
    tx: &mut Transaction,
    header_id: &str,
    height: i32,
    invalidation_height: Option<i32>,
) {
    tx.execute(
        "
        insert into cex.block_processing_log (
            header_id,
            height,
            invalidation_height,
            status
        )
        values ($1, $2, $3, 'pending');
        ",
        &[&header_id, &height, &invalidation_height],
    )
    .unwrap();
}

/// Adjust invalidation height of specified block
///
/// Used when removing/restoring conflicting cex addresses.
pub(super) fn update_invalidation_height(
    tx: &mut Transaction,
    header_id: &String,
    invalidation_height: Option<i32>,
) {
    tx.execute(
        "
        update cex.block_processing_log
        set invalidation_height = $2
        where header_id = $1;
        ",
        &[&header_id, &invalidation_height],
    )
    .unwrap();
}

/// Removes block from log if still pending.
///
/// Noop for any other block status.
fn delete_pending_block(tx: &mut Transaction, header_id: &str) {
    tx.execute(
        "
        delete from cex.block_processing_log
        where header_id = $1 and status = 'pending';",
        &[&header_id],
    )
    .unwrap();
}

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

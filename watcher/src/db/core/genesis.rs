use crate::parsing::Output;

const ZERO: i32 = 0;
const ZERO_VOTE: i16 = 0;
const ZERO_HEADER: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const GENESIS_HEADER_ID: &str = ZERO_HEADER;
const GENESIS_HEADER_PARENT_ID: &str = "genesis";
const GENESIS_TIMESTAMP: i64 = 1561978800000;
const DIFFICULTY: i64 = 0;
const GENESIS_TX_ID: &str = ZERO_HEADER;

/// Helper function to include genesis boxes in db.
///
/// Includes dummy header and tx to satisfy FK's.
pub fn include_genesis_boxes(tx: &mut postgres::Transaction, boxes: &Vec<Output>) {
    // Genesis header
    tx.execute(
        "
        insert into core.headers (height, id, parent_id, timestamp, difficulty, vote1, vote2, vote3)
        values ($1, $2, $3, $4, $5, $6, $7, $8);",
        &[
            &ZERO,
            &GENESIS_HEADER_ID,
            &GENESIS_HEADER_PARENT_ID,
            &GENESIS_TIMESTAMP,
            &DIFFICULTY,
            &ZERO_VOTE,
            &ZERO_VOTE,
            &ZERO_VOTE,
        ],
    )
    .unwrap();

    // Genesis tx
    tx.execute(
        "
        insert into core.transactions (id, header_id, height, index)
        values ($1, $2, $3, $4);",
        &[&GENESIS_TX_ID, &GENESIS_HEADER_ID, &ZERO, &ZERO],
    )
    .unwrap();

    // Outputs and registers
    super::outputs::include_genesis_boxes(tx, &boxes, GENESIS_HEADER_ID, GENESIS_TX_ID);
    super::additional_registers::include_genesis_boxes(tx, &boxes);
}

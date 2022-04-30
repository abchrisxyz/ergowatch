use crate::parsing::Output;

const ZERO: i32 = 0;
const ZERO_HEADER: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const GENESIS_HEADER_ID: &str = ZERO_HEADER;
const GENESIS_HEADER_PARENT_ID: &str = "genesis";
const GENESIS_TIMESTAMP: i64 = 1561978800000;
const GENESIS_TX_ID: &str = ZERO_HEADER;

/// Helper function to include genesis boxes in db.
///
/// Includes dummy header and tx to satisfy FK's.
pub fn include_genesis_boxes(
    tx: &mut postgres::Transaction,
    boxes: &Vec<crate::node::models::Output>,
) {
    // Genesis header
    tx.execute(
        "
        insert into core.headers (height, id, parent_id, timestamp)
        values ($1, $2, $3, $4);",
        &[
            &ZERO,
            &GENESIS_HEADER_ID,
            &GENESIS_HEADER_PARENT_ID,
            &GENESIS_TIMESTAMP,
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
    let outputs = boxes.iter().map(|b| Output::from_node_output(b)).collect();
    super::outputs::include_genesis_boxes(tx, &outputs, GENESIS_HEADER_ID, GENESIS_TX_ID);
    super::additional_registers::include_genesis_boxes(tx, &outputs);
}

// // repair event utils
// use postgres::Transaction;

// /// Prepare a repair cycle.
// pub fn prepare(tx: &mut Transaction) {
//     // Make a working copy of new deposit addresses.

//     // Set pending(_rollback) to processing_(rollback)

//     // Delete from cex.addresses for any pending_rollback block

//     // Add new deposit addresses to cex.addresses.

//     // Clear new deposit addresses.

//     // Record current sync height, so we know when to stop.

//     let sql = "
//         create table cex._repair (
//             from_height int,
//             to_height int,
//             started timestamp,
//         )
//     ";

//     todo!()
// }

// /// Start repair thread
// pub fn start() {
//     // loop over heights

//     // Set processing(_rollback) to processed(_rollback)
//     // If processing block got rolled back during the repair event,
//     // it will be marked as pending_rollback and will be picked up
//     // during next repair event.
// }

// fn repair(height: i32) {

//     // repair_cex_supply()
//     // repair_metrics()
//     // record progress

//     // commit
// }

// fn repair_cex_supply() {
//     todo!()
// }

// fn repair_metrics() {
//     todo!()
// }

mod db;
mod node;
mod parsing;
mod session;
mod settings;
mod sync;
mod types;
use session::Session;

fn main() -> Result<(), &'static str> {
    let mut session = Session::new()?;

    if session.db.is_empty().unwrap() {
        if !session.allow_bootstrap {
            session.db.apply_constraints_all().unwrap();
        }
        sync::include_genesis_boxes(&session)?;
    };

    if session.allow_bootstrap {
        let db_constraints_status = session.db.constraints_status().unwrap();
        if !db_constraints_status.tier_1 {
            sync::bootstrap::phase_1(&mut session).unwrap();
        }
        if !sync::bootstrap::db_is_bootstrapped(&session) {
            sync::bootstrap::phase_2(&mut session).unwrap();
            // Bootstrapping is completed, allow rollbacks now.
            session.allow_rollbacks = true;
        }
    }

    // Main loop
    sync::sync_and_track(&mut session).unwrap();
    Ok(())
}

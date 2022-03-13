mod db;
mod node;
mod session;
mod settings;
mod sync;
mod types;
mod units;
use session::Session;

fn main() -> Result<(), &'static str> {
    let mut session = Session::new()?;

    if session.db.is_empty().unwrap() {
        if !session.allow_bootstrap {
            session.load_db_constraints()?;
        }
        sync::include_genesis_boxes(&session)?;
    };

    if session.allow_bootstrap {
        if !session.db_constraints_set {
            sync::bootstrap::sync_core(&mut session).unwrap();
        }
        if !sync::bootstrap::db_is_bootstrapped(&session) {
            sync::bootstrap::expand_db(&mut session).unwrap();
        }
    }

    // Main loop
    sync::sync_and_track(&mut session).unwrap();
    Ok(())
}

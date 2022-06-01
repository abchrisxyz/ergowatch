mod cache;
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

    // Bootstrap if needed
    sync::bootstrap::run(&mut session).unwrap();

    // TODO: move cache under db
    session.cache = session.db.load_cache();

    // Main loop
    sync::sync_and_track(&mut session).unwrap();
    Ok(())
}

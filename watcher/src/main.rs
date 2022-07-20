mod coingecko;
mod constants;
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

    // Init DB cache
    session.db.load_cache();

    // Bootstrap if needed
    sync::bootstrap::run(&mut session).unwrap();

    // Main loop
    sync::sync_and_track(&mut session).unwrap();
    Ok(())
}

mod db;
mod node;
mod settings;
mod types;
mod units;

use log::debug;
use log::error;
use log::info;
use std::{thread, time};

mod session;
use session::prepare_session;

// TODO: move this to config
const POLL_INTERVAL_SECONDS: u64 = 5;

fn main() -> Result<(), &'static str> {
    let mut session = prepare_session()?;

    // TODO: avoid doing potentially more than once on rerun
    if session.head.height == 0 {
        session.include_genesis_boxes().unwrap();
    }

    loop {
        let node_height = match session.node.get_height() {
            Ok(h) => h,
            Err(e) => {
                error!("{}", e);
                info!("Retrying in {} seconds", POLL_INTERVAL_SECONDS);
                thread::sleep(time::Duration::from_secs(POLL_INTERVAL_SECONDS));
                continue;
            }
        };

        if node_height <= session.head.height {
            if session.bootstrapping {
                // TODO: start/resume bootstrap process for non-core tables
            }
            if session.sync_once {
                debug!("Done syncing, exiting now");
                return Ok(());
            }
            debug!("No new blocks - waiting {} seconds", POLL_INTERVAL_SECONDS);
            thread::sleep(time::Duration::from_secs(POLL_INTERVAL_SECONDS));
            continue;
        }

        session.sync_to(node_height)?;

        if session.bootstrapping {
            info!("Proceeding with bootstrap process");
            session.load_db_constraints()?;
            session.run_bootstrapping_queries()?;
            info!("Bootstrapping completed - proceeding in normal mode");
            return Ok(());
        } else {
            info!("Database is synced - waiting for next block")
        }
    }
}

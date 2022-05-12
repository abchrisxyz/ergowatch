/*
   Repair events.
*/
use super::DB;
use crate::db::cexs;
use crate::db::metrics;
use log::debug;
use log::info;
use log::warn;
use postgres::{Client, NoTls};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time;

pub enum RepairInitError {
    /// Another repair event is still running
    OtherRunning,
}

/// MPSC messages between RepairEvent and spawn thread
enum Message {
    Abort,
}

#[derive(Debug)]
pub struct RepairEvent {
    /// First height to repair
    fr_height: i32,
    /// Last height to repair
    to_height: i32,
    /// DB connection string
    conn_str: String,
    /// Used to track spawn thread
    tracer: Option<Arc<()>>,
    /// MPSC sender to spawn thread
    tx: Option<mpsc::Sender<Message>>,
}

impl RepairEvent {
    // fn new(fr_height: i32, to_height: i32) -> Result<Self, RepairError> {
    //     let statements = vec![];
    //     todo!()
    // }

    /// Start repair.
    ///
    /// Spawns a new thread and returns.
    ///
    pub fn start(&mut self) {
        debug!("Repair event starting");
        // Check for running thread
        if self.tracer.is_some() {
            warn!("Tried to start an already started repair event");
            return;
        }
        let conn_str = String::from(&self.conn_str);
        // Copy height params to be moved into thread
        let fr = self.fr_height;
        let to = self.to_height;
        // Prepare message passing channel
        let (tx, rx) = mpsc::channel();
        self.tx = Some(tx);
        // Track tracer livespan
        let tracker = Arc::new(());
        let tracer = tracker.clone();
        self.tracer = Some(tracker);
        thread::spawn(move || {
            let _t = tracer;
            debug!("Repair thread started");
            start(conn_str, fr, to, rx).unwrap();
        });
    }

    pub fn is_running(&self) -> bool {
        if let Some(tracer) = &self.tracer {
            return Arc::strong_count(&tracer) > 1;
        }
        warn!("Calling is_running on RepairEvent that wasn't started yet");
        false
    }

    /// Abort repair event.
    ///
    /// Blocks until repair is aborted.
    pub fn abort(&self) {
        info!("Aborting repair event");
        if let Some(tx) = &self.tx {
            match tx.send(Message::Abort) {
                Ok(_) => (),
                Err(err) => {
                    debug!("{:?}", err);
                    warn!("Tried to abort terminated repair event");
                    return;
                }
            };
            while self.is_running() {
                thread::sleep(time::Duration::from_secs(1))
            }
            info!("Repair event aborted");
        }
    }
}

impl DB {
    pub fn start_repair_event(&mut self, max_height: i32) {
        debug!("Starting repair event");
        let mut client = Client::connect(&self.conn_str, NoTls).unwrap();
        match prepare(&mut client) {
            Ok(()) => (),
            Err(RepairInitError::OtherRunning) => {
                warn!("Tried to start a repair event but previous one is still running. Consider using a larger repair interval.");
                return;
            }
        };
        let start_height = match cexs::repair::get_start_height(&mut client, max_height) {
            Some(h) => h,
            None => {
                info!("No pending repairs");
                cleanup(&mut client);
                self.repair_event = None;
                return;
            }
        };
        let mut e = RepairEvent {
            fr_height: start_height,
            to_height: max_height,
            conn_str: String::from(&self.conn_str),
            tracer: None,
            tx: None,
        };
        e.start();
        self.repair_event = Some(e);
    }

    /// Returns true if a repair event is running and set to process given height.
    pub fn is_repairing_height(&self, height: i32) -> bool {
        if let Some(e) = &self.repair_event {
            return e.to_height >= height;
        }
        false
    }

    /// Blocks until any running repairs are finished.
    pub fn wait_for_repairs(&self) {
        if let Some(e) = &self.repair_event {
            info!("Waiting for repairs to complete");
            while e.is_running() {
                thread::sleep(time::Duration::from_secs(1));
            }
        }
    }

    /// Abort any running repairs
    pub fn abort_repairs(&self) {
        info!("Aborting repair session");
        if let Some(e) = &self.repair_event {
            e.abort();
            let mut client = Client::connect(&self.conn_str, NoTls).unwrap();
            cleanup(&mut client);
        }
    }
}

/// Prepare a repair session
///
/// Will fail if another repair session is running
fn prepare(client: &mut Client) -> Result<(), RepairInitError> {
    let mut tx = client.transaction().unwrap();
    match tx.execute("create table ew._repair as select now() as created;", &[]) {
        Ok(_) => (),
        Err(err) => {
            if let Some(&postgres::error::SqlState::DUPLICATE_TABLE) = err.code() {
                return Err(RepairInitError::OtherRunning);
            }
            panic!("{:?}", err);
        }
    };
    tx.commit().unwrap();
    Ok(())
}

/// Start a previously prepared repair session
fn start(conn_str: String, fr: i32, to: i32, rx: mpsc::Receiver<Message>) -> anyhow::Result<()> {
    info!("Repairing {} blocks ({} to {})", to - fr + 1, fr, to);
    let mut client = Client::connect(&conn_str, NoTls).unwrap();

    let mut tx = client.transaction()?;
    cexs::repair::process_non_invalidating_blocks(&mut tx);
    tx.commit().unwrap();

    for h in fr..to + 1 {
        let mut tx = client.transaction()?;

        if let Ok(Message::Abort) = rx.try_recv() {
            break;
        }

        metrics::repair(&mut tx, h);
        cexs::repair::set_height_pending_to_processed(&mut tx, h);

        // Commit as we progress
        tx.commit().unwrap();
    }

    cleanup(&mut client);

    info!("Done repairing heights {} to {}", fr, to);
    Ok(())
}

/// Cleanup
fn cleanup(tx: &mut Client) {
    debug!("Cleaning up repair session");
    tx.execute("drop table if exists ew._repair;", &[]).unwrap();
}

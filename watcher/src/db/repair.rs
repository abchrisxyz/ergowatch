/*
   Repair events.

   Some blocks will invalidate previously derived data. When this happens,
   impacted data must be derived again using the newly availble data.
   This process is taken care of by repair events. Repair events are
   performed periodically, say every 100 blocks for instance, in a dedicated
   thread.

   A repair event will start from the lowest impacted height and go over
   all heuight until the current height minus a configurable threshold of
   a few blocks. Stopping repairs a few blocks from the latest reduces
   the chance of including blocks that will be rolled back eventually.

   A repair event consists of two phases: a preparation phase and
   an execution phase. The preparation phase creates work tables representing
   the db state at the repair start height. The execution phase goes over the
   height range to be repaired, fixing derived data and updating the state of
   the work tables.

   At this stage there is only one thing that invalidates previously derived
   data: the discovery of new exchange deposit addresses. Deposit addresses
   are only identified when sending funds to a main exchange address.
   Obviously, the funding of the deposit address occcured prior to that and
   needs to be reflected in data depending on it - cex supply and all metrics
   accounting for supply on exchanges.
*/
use super::DB;
use crate::db::addresses;
use crate::db::cexs;
use crate::db::metrics;
use log::debug;
use log::info;
use log::warn;
use postgres::{Client, NoTls, Transaction};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time;

pub const REPLAY_ID: &str = "repair";

pub enum RepairInitError {
    /// Another repair event is still running
    OtherRunning,
}

/// MPSC messages from RepairEvent to spawn thread
enum Message {
    Abort,
    Pause,
    Resume,
}

/// MPSC messages to RepairEvent from spawn thread
enum Response {
    Paused,
    Resumed,
}

#[derive(Debug)]
pub struct RepairEvent {
    /// Repair session settings
    range: RepairRange,
    /// DB connection string
    conn_str: String,
    /// Used to track spawn thread
    tracer: Option<Arc<()>>,
    /// MPSC sender to spawn thread
    tx: Option<mpsc::Sender<Message>>,
    /// MPSC sender to spawn thread
    rx: Option<mpsc::Receiver<Response>>,
}

#[derive(Debug, Clone, Copy)]
pub struct RepairRange {
    /// First height to repair
    first_height: i32,
    /// Last height to repair
    last_height: i32,
}

impl RepairRange {
    pub fn new(first_height: i32, last_height: i32) -> Self {
        Self {
            first_height,
            last_height,
        }
    }
}

impl RepairEvent {
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
        let range = self.range;
        // Prepare message passing channels
        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();
        self.tx = Some(tx1);
        self.rx = Some(rx2);
        // Track tracer livespan
        let tracker = Arc::new(());
        let tracer = tracker.clone();
        self.tracer = Some(tracker);
        thread::spawn(move || {
            let _t = tracer;
            debug!("Repair thread started");
            // start(conn_str, fr, to, prepare_replay, rx1, tx2).unwrap();
            start(conn_str, range, rx1, tx2).unwrap();
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

    /// Pause repair event.
    ///
    /// Blocks until repair is paused.
    pub fn pause(&self) {
        info!("Pausing repair event");
        if let Some(tx) = &self.tx {
            // Send signal
            match tx.send(Message::Pause) {
                Ok(_) => (),
                Err(err) => {
                    debug!("{:?}", err);
                    warn!("Tried to pause terminated repair event");
                    return;
                }
            };
            // Now wait for pause ankownledgement or end of channel
            // if pause request was sent during last pass.
            if let Some(rx) = &self.rx {
                match rx.recv() {
                    Ok(Response::Paused) => info!("Repair event paused"),
                    Ok(Response::Resumed) => {
                        panic!("Received unexpected resumed signal from repair thread")
                    }
                    Err(rcv_error) => {
                        info!(
                            "Repair event finished before it could be paused ({})",
                            rcv_error
                        )
                    }
                }
            }
        }
    }

    /// Resume paused repair event.
    ///
    /// Blocks until repair is resumed.
    pub fn resume(&self) {
        info!("Resuming repair event");
        if let Some(tx) = &self.tx {
            // Send signal
            match tx.send(Message::Resume) {
                Ok(_) => (),
                Err(err) => {
                    debug!("{:?}", err);
                    info!("Tried to resume terminated repair event (likely before it completed before it could be paused)");
                    return;
                }
            };
            // Now wait for resume ankownledgement
            if let Some(rx) = &self.rx {
                match rx.recv() {
                    Ok(Response::Resumed) => info!("Repair event resumed"),
                    Ok(Response::Paused) => {
                        panic!("Received unexpected pause signal from repair thread")
                    }
                    Err(rcv_error) => {
                        info!(
                            "Repair event finished before resuming could be confirmed ({})",
                            rcv_error
                        )
                    }
                }
            }
        }
    }
}

impl DB {
    /// Start a new repair session
    ///
    /// Initializes a new session and starts it.
    pub fn start_new_repair_event(&mut self, max_height: i32) {
        debug!("Starting new repair event");
        let mut client = Client::connect(&self.conn_str, NoTls).unwrap();
        let start_height = match cexs::repair::get_start_height(&mut client, max_height) {
            Some(h) => h,
            None => {
                info!("No pending repairs");
                return;
            }
        };
        match init(&mut client, start_height, max_height) {
            Ok(()) => (),
            Err(RepairInitError::OtherRunning) => {
                warn!("Tried to start a repair event but previous one is still running. Consider using a larger repair interval.");
                return;
            }
        };
        self.start_repair_event(RepairRange::new(start_height, max_height));
    }

    /// Cleanup existing session or resume it if `resume` is true.
    ///
    /// Does nothing if there aren't any stopped repair sessions.
    pub fn purge_or_resume(&mut self, resume: bool) {
        // Can only ever be called at startup, where no repairs are running yet
        assert!(self.repair_event.is_none());

        let mut client = Client::connect(&self.conn_str, NoTls).unwrap();

        // Check for interrupted session settings
        let repair_range: Option<RepairRange> = match client
            .query_opt("select next_height, last_height from ew.repairs;", &[])
            .unwrap()
        {
            Some(row) => Some(RepairRange::new(row.get(0), row.get(1))),
            None => None,
        };

        if resume {
            // Resume session if any
            match repair_range {
                Some(range) => {
                    info!("Resuming existing repair session");
                    self.start_repair_event(range);
                }
                None => info!("No repair session to resume"),
            }
        } else if repair_range.is_some() {
            info!("Cleaning up interrupted repair session");
            cleanup(&mut client);
        };
    }

    fn start_repair_event(&mut self, range: RepairRange) {
        debug!("Starting repair event");
        let mut e = RepairEvent {
            range,
            conn_str: String::from(&self.conn_str),
            tracer: None,
            tx: None,
            rx: None,
        };
        e.start();
        self.repair_event = Some(e);
    }

    /// Returns true if a repair event is running
    pub fn is_repairing(&self) -> bool {
        if let Some(e) = &self.repair_event {
            e.is_running()
        } else {
            false
        }
    }

    /// Returns true if a repair event is running and set to process given height.
    pub fn is_repairing_height(&self, height: i32) -> bool {
        if let Some(e) = &self.repair_event {
            return e.range.last_height >= height;
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

    /// Pause running repairs
    pub fn pause_repairs(&self) {
        if let Some(e) = &self.repair_event {
            e.pause();
        }
    }

    /// Resume paused repairs
    pub fn resume_repairs(&self) {
        if let Some(e) = &self.repair_event {
            e.resume();
        }
    }

    /// Drop existing replay tables
    pub fn drop_replay_tables(&self) {
        let mut client = Client::connect(&self.conn_str, NoTls).unwrap();
        let mut tx = client.transaction().unwrap();
        drop_replay_tables(&mut tx);
        tx.commit().unwrap();
    }
}

/// Initialize a repair session on the db side.
///
/// Will fail if another repair session is running.
fn init(client: &mut Client, start_height: i32, max_height: i32) -> Result<(), RepairInitError> {
    info!("Creating a repair session for blocks {start_height} to {max_height}");
    let mut tx = client.transaction().unwrap();
    // Lock repair session (prevents others from starting)
    // Log creation timestamp - usefull for debugging
    match tx.execute(
        "
        insert into ew.repairs (started, from_height, last_height, next_height)
        select now(), $1, $2, $1;",
        &[&start_height, &max_height],
    ) {
        Ok(_) => (),
        Err(err) => {
            if let Some(&postgres::error::SqlState::UNIQUE_VIOLATION) = err.code() {
                return Err(RepairInitError::OtherRunning);
            }
            panic!("{:?}", err);
        }
    };
    tx.commit().unwrap();
    Ok(())
}

/// Start a previously prepared repair session
///
/// `fr`: Height of first block to be repaired
/// `to`: Height of last block to be repaired
/// `prepare_replay`: Create replay tables or not
///     
fn start(
    conn_str: String,
    range: RepairRange,
    channel_rx: mpsc::Receiver<Message>,
    channel_tx: mpsc::Sender<Response>,
) -> anyhow::Result<()> {
    info!(
        "[thread] Repairing heights {} to {} ({} blocks)",
        range.first_height,
        range.last_height,
        range.last_height - range.first_height + 1
    );
    let mut client = Client::connect(&conn_str, NoTls).unwrap();

    // Load caches of state just prior to start height
    let mut cex_cache = cexs::Cache::load_at(&mut client, range.first_height - 1);

    // Mark non-invalidating blocks as processed
    let mut tx = client.transaction()?;
    cexs::repair::process_non_invalidating_blocks(&mut tx);
    tx.commit().unwrap();

    // Prepare work tables
    info!("[thread] Preparing replay tables");
    let mut tx = client.transaction()?;
    prepare(&mut tx, range.first_height - 1);
    tx.commit().unwrap();

    // Counter to log exact number of repaired heights.
    let mut processed_heights_counter = 0;

    info!("[thread] Starting repairs");
    for h in range.first_height..range.last_height + 1 {
        // Check for incoming messages
        match channel_rx.try_recv() {
            // Abort repair session
            Ok(Message::Abort) => {
                info!("[thread] Repair session received abort signal");
                break;
            }
            // Pause repair session and wait for next message
            Ok(Message::Pause) => {
                info!("[thread] Repair session received pause signal");
                channel_tx.send(Response::Paused).unwrap();
                match wait_for_message(&channel_rx) {
                    Message::Abort => break,
                    Message::Pause => {
                        panic!(
                            "[thread] Repair session received pause signal while already paused"
                        );
                    }
                    Message::Resume => {
                        info!("[thread] Repair session received resume signal");
                        channel_tx.send(Response::Resumed).unwrap();
                    }
                };
            }
            // Should not happen
            Ok(Message::Resume) => {
                warn!("[thread] Repair sessions received resume signal while already running")
            }
            // No messages, continue repair session
            Err(_) => (),
        }

        let mut tx = client.transaction()?;

        // Advance state of work tables to current height
        step(&mut tx, h);

        cexs::repair(&mut tx, h, &mut cex_cache);
        metrics::repair(&mut tx, h);
        cexs::repair::set_height_pending_to_processed(&mut tx, h);

        // Log progress
        let next_height = h + 1;
        // Last iteration sets next_height > to
        if next_height != range.last_height + 1 {
            tx.execute("update ew.repairs set next_height = $1+1;", &[&h])?;
        }

        // Commit as we progress
        tx.commit().unwrap();
        processed_heights_counter += 1;
    }

    info!("[thread] Cleaning up repair session");
    cleanup(&mut client);

    assert_eq!(
        processed_heights_counter,
        range.last_height + 1 - range.first_height
    );
    info!(
        "[thread] Done repairing heights {} to {}",
        range.first_height, range.last_height
    );

    Ok(())
}

/// Create work tables for repair session.
fn prepare(tx: &mut Transaction, at_height: i32) {
    debug!("Preparing replay tables for repair session");
    addresses::replay::prepare(tx, at_height, REPLAY_ID);
}

/// Create work tables for repair session.
fn step(tx: &mut Transaction, next_height: i32) {
    addresses::replay::step(tx, next_height, REPLAY_ID);
}

/// Cleanup
fn cleanup(client: &mut Client) {
    debug!("Cleaning up repair session");
    let mut tx = client.transaction().unwrap();
    drop_replay_tables(&mut tx);
    tx.execute("truncate table ew.repairs;", &[]).unwrap();
    tx.commit().unwrap();
}

fn wait_for_message(rx: &mpsc::Receiver<Message>) -> Message {
    loop {
        thread::sleep(time::Duration::from_secs(1));
        if let Ok(msg) = rx.try_recv() {
            return msg;
        };
    }
}

fn drop_replay_tables(tx: &mut Transaction) {
    addresses::replay::cleanup(tx, REPLAY_ID);
}

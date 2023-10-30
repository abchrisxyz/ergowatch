use axum::extract::Extension;
use axum::routing::get;
use axum::Json;
use axum::Router;
use serde::Serialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;

use crate::core::types::Height;

#[derive(Debug)]
pub enum MonitorMessage {
    Worker(WorkerMessage),
    Cursor(CursorMessage),
    /// Holds name of dropped cursor
    CursorDrop(String),
    Rollback(CursorRollback),
}

#[derive(Debug)]
pub struct CursorMessage {
    name: String,
    height: Height,
}

impl CursorMessage {
    pub fn new(name: String, height: Height) -> Self {
        Self { name, height }
    }
}

#[derive(Debug)]
pub struct CursorRollback {
    name: String,
    height: Height,
}

impl CursorRollback {
    pub fn new(name: String, height: Height) -> Self {
        Self { name, height }
    }
}

#[derive(Debug, Serialize)]
pub struct WorkerMessage {
    name: String,
    height: Height,
}

impl WorkerMessage {
    pub fn new(name: String, height: Height) -> Self {
        Self { name, height }
    }
}

#[derive(Default, Serialize)]
struct MonitorData {
    /// Cursor specific timers
    cursors: HashMap<String, CursorStatus>,
    /// Workers
    workers: HashMap<String, Height>,
}

#[derive(Serialize, Clone)]
struct CursorStatus {
    height: Height,
    blocks_since_start: i32,
    rollbacks_since_start: i32,
    /// Average blocks per second since start
    bps_since_start: f32,
    /// Blocks per second for last 100 blocks
    bps_last_100: f32,

    #[serde(skip_serializing)]
    timer_since_start: std::time::Instant,
    #[serde(skip_serializing)]
    timer_last_100: std::time::Instant,
}

impl CursorStatus {
    pub fn new() -> Self {
        CursorStatus {
            height: 0,
            blocks_since_start: 0,
            rollbacks_since_start: 0,
            bps_since_start: 0f32,
            bps_last_100: 0f32,
            timer_since_start: std::time::Instant::now(),
            timer_last_100: std::time::Instant::now(),
        }
    }

    /// Save time elapsed since last update
    pub fn update(&mut self, height: Height) {
        self.height = height;
        self.blocks_since_start += 1;
        self.bps_since_start =
            self.blocks_since_start as f32 / self.timer_since_start.elapsed().as_secs_f32();
        if self.blocks_since_start % 100 == 0 {
            self.bps_last_100 = 100f32 / self.timer_last_100.elapsed().as_secs_f32();
            self.timer_last_100 = std::time::Instant::now();
        }
    }

    pub fn log_rollback(&mut self, _height: Height) {
        self.rollbacks_since_start += 1;
    }
}

type SharedState = Arc<RwLock<MonitorData>>;

pub struct Monitor {
    tx: mpsc::Sender<MonitorMessage>,
    rx: mpsc::Receiver<MonitorMessage>,
}

impl Monitor {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(32);
        Self { tx, rx }
    }

    pub fn sender(&self) -> mpsc::Sender<MonitorMessage> {
        self.tx.clone()
    }

    pub async fn start(&mut self) {
        let state = SharedState::default();

        self.start_server(state.clone());

        loop {
            match self.rx.recv().await.expect("some message") {
                MonitorMessage::Cursor(msg) => {
                    let mut data = state.write().unwrap();
                    data.cursors
                        .entry(msg.name.clone())
                        .and_modify(|cs| cs.update(msg.height))
                        .or_insert(CursorStatus::new());
                }
                MonitorMessage::CursorDrop(cursor_name) => {
                    let mut data = state.write().unwrap();
                    data.cursors.remove(&cursor_name).unwrap();
                }
                MonitorMessage::Worker(msg) => {
                    let mut data = state.write().unwrap();
                    data.workers
                        .entry(msg.name)
                        .and_modify(|h| *h = msg.height)
                        .or_insert(msg.height);
                }
                MonitorMessage::Rollback(msg) => {
                    let mut data = state.write().unwrap();
                    data.cursors
                        .entry(msg.name.clone())
                        .and_modify(|cs| cs.log_rollback(msg.height))
                        .or_insert(CursorStatus::new());
                }
            };
        }
    }

    fn start_server(&self, state: SharedState) {
        let app = Router::new()
            .route(
                "/",
                get(|| async { "Hey there, you're probably after /status" }),
            )
            .route("/status", get(status))
            .layer(Extension(state));

        let address = SocketAddr::from(([0, 0, 0, 0], 3005));
        tokio::spawn(async move {
            tracing::info!("listening on {}", &address);

            axum::Server::bind(&address)
                .serve(app.into_make_service())
                .await
                .unwrap()
        });
    }
}

#[derive(Serialize)]
struct Status {
    // cursors: Vec<CursorStatus>,
    cursors: HashMap<String, CursorStatus>,
    workers: Vec<WorkerMessage>,
}

async fn status(Extension(state): Extension<SharedState>) -> Json<Status> {
    let data = &state.read().unwrap();
    // let cursors: Vec<CursorStatus> = data.cursors.values().cloned().collect();
    let cursors: HashMap<String, CursorStatus> = data.cursors.clone();
    let workers: Vec<WorkerMessage> = data
        .workers
        .iter()
        .map(|(k, v)| WorkerMessage::new(k.clone(), *v))
        .collect();
    Json(Status { cursors, workers })
}

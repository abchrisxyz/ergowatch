use axum::extract::Extension;
use axum::routing::get;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::mpsc;

use crate::core::types::Height;

#[derive(Debug)]
pub enum MonitorMessage {
    Worker(Height),
    Cursor(CursorMessage),
}

#[derive(Debug)]
pub struct CursorMessage {
    // name: String,
    height: Height,
    time_node_mus: u128,
    time_store_mus: u128,
    time_total_mus: u128,
}

impl CursorMessage {
    pub fn new(
        // name: String,
        height: Height,
        time_node_mus: u128,
        time_store_mus: u128,
        time_total_mus: u128,
    ) -> Self {
        Self {
            // name,
            height,
            time_node_mus,
            time_store_mus,
            time_total_mus,
        }
    }
}

// TODO: supoprt multiple cursors
#[derive(Default)]
struct MonitorData {
    /// Height of last processed block
    pub core_height: Height,
    pub blocks_since_start: i32,
    pub core_micros_node: u128,
    pub core_micros_store: u128,
    pub core_micros_total: u128,
    /// Height of last processed block
    pub sigmausd_height: Height,
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
                MonitorMessage::Cursor(cm) => {
                    let mut data = state.write().unwrap();
                    data.core_height = cm.height;
                    data.blocks_since_start += 1;
                    data.core_micros_node += cm.time_node_mus;
                    data.core_micros_store += cm.time_store_mus;
                    data.core_micros_total += cm.time_total_mus;
                }
                MonitorMessage::Worker(h) => {
                    state.write().unwrap().sigmausd_height = h;
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

async fn status(Extension(state): Extension<SharedState>) -> String {
    let data = &state.read().unwrap();

    format!(
        "core height: {}\nsigmausd:    {}\n\nblocks since start: {}\n\ncursor timers:\n  node : {}s\n  store: {}s\n  total: {}s",
        data.core_height, data.sigmausd_height, data.blocks_since_start, data.core_micros_node / 1_000_000, data.core_micros_store / 1_000_000, data.core_micros_total / 1_000_000
    )
}

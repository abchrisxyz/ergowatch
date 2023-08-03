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
    WorkerUpdate(Height),
    CoreUpdate(Height),
}

#[derive(Default)]
struct MonitorData {
    /// Height of last processed block
    pub core_height: Height,
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
                MonitorMessage::CoreUpdate(h) => {
                    state.write().unwrap().core_height = h;
                }
                MonitorMessage::WorkerUpdate(h) => {
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
        "core height: {}\nsigmausd:    {}",
        data.core_height, data.sigmausd_height
    )
}

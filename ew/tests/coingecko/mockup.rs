use std::sync::Arc;
use tokio::sync::oneshot;

use axum::extract::Query;
use axum::extract::State;
use axum::response::Json;
use axum::routing;
use axum::Router;
use serde_json::json;
use serde_json::Value;

const MOCK_API_PORT: i32 = 9055;

pub type APIData = Vec<(i64, f32)>;

async fn wait_some() {
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
}

/// Coingecko api mockup
pub struct MockGecko {
    url: String,
    term_tx: Option<oneshot::Sender<()>>,
}

impl MockGecko {
    pub fn new() -> Self {
        Self {
            url: format!("http://{}/ergusd", Self::address()),
            term_tx: None,
        }
    }

    /// Address for internal server configuration
    fn address() -> String {
        format!("127.0.0.1:{MOCK_API_PORT}")
    }

    /// External url to be queried by clients
    pub fn get_url(&self) -> &str {
        &self.url
    }

    /// Serve given `data`
    pub async fn serve(&mut self, data: APIData) {
        if self.term_tx.is_some() {
            panic!("Can't start a MockGecko that's already running");
        }
        let shared_state = Arc::new(data);

        let app = Router::new()
            .route("/ergusd", routing::get(query_range))
            .with_state(shared_state);

        let address = Self::address();
        let (tx, rx) = oneshot::channel();
        self.term_tx = Some(tx);
        tokio::spawn(async move {
            tracing::info!("Starting MockGecko server listening on {}", &address);
            tokio::select! {
                _ = axum::Server::bind(&address.parse().unwrap())
                    .serve(app.into_make_service()) => {},
                _ = rx => {tracing::info!("Stopped server")},
            }
        });
        // Wait some to ensure server is ready
        wait_some().await;
    }

    /// Stop serving.
    ///
    /// Usefull when wanting to serve other data.
    pub async fn stop(&mut self) {
        match self.term_tx.take() {
            Some(tx) => {
                tx.send(()).unwrap();
                wait_some().await;
            }
            None => (),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct RangeParameters {
    from: i64,
    to: i64,
}

#[derive(Debug, serde::Serialize)]
struct QueryRangeResponse {
    prices: APIData,
    market_caps: APIData,
    total_volumes: APIData,
}

/// Mock of `https://api.coingecko.com/api/v3/coins/ergo/market_chart/range`
async fn query_range(
    State(data): State<Arc<APIData>>,
    params: Query<RangeParameters>,
) -> Json<Value> {
    tracing::debug!("query_range {params:?}");
    // Convert time range from s to ms
    let fr_ms = params.from * 1000;
    let to_ms = params.to * 1000;
    let records: APIData = data
        .iter()
        .filter(|r| r.0 >= fr_ms && r.0 <= to_ms)
        .map(|r| (r.0, r.1))
        .collect();
    let res = QueryRangeResponse {
        prices: records,
        market_caps: vec![],
        total_volumes: vec![],
    };
    Json(json!(res))
}

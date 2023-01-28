use super::blocks::TestBlock;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::response::Json;
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
// use tokio::sync::mpsc;
use tokio::sync::oneshot;

// Default port of mock api
const DEFAULT_PORT: i32 = 9053;
const LOCALHOST: &str = "127.0.0.1";

type HeaderID = String;
type BlockIndex = usize;

async fn wait_some() {
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
}

pub struct TestNode {
    address: String,
    url: String,
    term_tx: Option<oneshot::Sender<()>>,
}

/// A mock node API listening on localhost.
impl TestNode {
    /// Create a new node on default port
    pub fn new() -> Self {
        Self {
            address: format!("{LOCALHOST}:{DEFAULT_PORT}"),
            url: format!("http://{LOCALHOST}:{DEFAULT_PORT}"),
            term_tx: None,
        }
    }

    /// Create a new node on the default port and start the api server.
    pub async fn run(block_ids: &[&str]) -> Self {
        let mut n = Self::new();
        n.serve(block_ids).await;
        // Give some time to server to start up
        wait_some().await;
        n
    }

    /// Return the node's api url
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Start the api server
    #[allow(dead_code)]
    pub async fn restart(&mut self, block_ids: &[&str]) {
        tracing::info!("Stopping server");
        match self.term_tx.take() {
            Some(tx) => tx.send(()).unwrap(),
            None => {
                panic!("Mock node server already stopped");
            }
        }
        self.serve(block_ids).await;
        wait_some().await;
    }

    async fn serve(&mut self, block_ids: &[&str]) {
        if self.term_tx.is_some() {
            panic!("Can't start a TestNode that's already running");
        }
        let data = APIData::new(block_ids);
        let shared_state = Arc::new(data);

        let app = Router::new()
            .route("/", get(|| async { "Hello, World!" }))
            .route("/blocks/at/:height", get(blocks_at))
            .route("/blocks/:header_id", get(blocks))
            .route("/blocks/:header_id/header", get(blocks_header))
            .route("/blocks/chainSlice", get(chain_slice))
            .with_state(shared_state);

        let address = self.address.to_string();
        let (tx, rx) = oneshot::channel();
        self.term_tx = Some(tx);
        tokio::spawn(async move {
            tracing::info!("Starting server listening to {}", &address);
            tokio::select! {
                _ = axum::Server::bind(&address.parse().unwrap())
                    .serve(app.into_make_service()) => {},
                _ = rx => {tracing::info!("Stopped server")},
            }
        });
    }
}

/// A mock node's preprocessed test data.
struct APIData {
    /// Collection of blocks known to the mock node
    blocks: Vec<TestBlock>,
    /// Height of each block, ordered as in `blocks`
    heights: Vec<i32>,
    /// Header id's of each block, ordered as in `blocks`
    header_ids: Vec<HeaderID>,
    /// Maps header ID's to an index into blocks
    lookup: HashMap<HeaderID, BlockIndex>,
    /// Main chain headers
    height_header_lookup: HashMap<i32, BlockIndex>,
}

impl APIData {
    fn new(block_ids: &[&str]) -> Self {
        // Collect blocks from id's
        let blocks: Vec<TestBlock> = block_ids.iter().map(|id| TestBlock::from_id(id)).collect();

        // Extract height from each block's header
        let heights: Vec<i32> = blocks.iter().map(|b| b.height()).collect();

        // Extract header id's from each block's header
        let header_ids: Vec<HeaderID> = blocks.iter().map(|b| b.header_id().to_string()).collect();

        // Building a header -> block-index lookup
        let lookup: HashMap<HeaderID, BlockIndex> =
            header_ids
                .iter()
                .enumerate()
                .fold(HashMap::new(), |mut acc, (i, h)| {
                    // Make sure we don't mask any blocks here.
                    // Insert returns None for new values.
                    assert_eq!(acc.insert(h.to_string(), i), None);
                    acc
                });

        // Building a header -> block-index lookup
        let height_header_lookup: HashMap<i32, BlockIndex> = block_ids
            .iter()
            .enumerate()
            // Keep main chain blocks only (not ending with *)
            .filter(|(_i, bid)| !bid.ends_with('*'))
            .fold(HashMap::new(), |mut acc, (i, _bid)| {
                // Make sure we don't mask any blocks here.
                // Insert returns None for new values.
                assert_eq!(acc.insert(heights[i], i), None);
                acc
            });

        Self {
            blocks,
            heights,
            header_ids,
            lookup,
            height_header_lookup,
        }
    }
}

/// Mock of `/blocks/at/<height>` node endpoint
///
/// Retruns collection of headers known for given `height`
async fn blocks_at(Path(height): Path<i32>, State(state): State<Arc<APIData>>) -> Json<Value> {
    let headers_ids: Vec<HeaderID> = state
        .heights
        .iter()
        .enumerate()
        .filter(|(_i, h)| **h == height)
        .map(|(i, _h)| state.header_ids[i].to_string())
        .collect();
    Json(json!(headers_ids))
}

/// Mock of `blocks/<header_id>`
///
/// Returns block data for given `header_id`
async fn blocks(Path(header_id): Path<String>, State(state): State<Arc<APIData>>) -> Json<Value> {
    let block_index = *state
        .lookup
        .get(&header_id)
        .expect(&format!("no such header in mock node lookup: {header_id}"));
    Json(state.blocks[block_index].to_json())
}

/// Mock of `blocks/<header_id>/header`
///
/// Returns header for given `header_id`
async fn blocks_header(
    Path(header_id): Path<String>,
    State(state): State<Arc<APIData>>,
) -> Json<Value> {
    let block_index = *state
        .lookup
        .get(&header_id)
        .expect("no such header in mock node lookup");
    Json(
        state.blocks[block_index]
            .to_json()
            .get("header")
            .unwrap()
            .clone(),
    )
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChainSliceParameters {
    from_height: i32,
    to_height: i32,
}

/// Mock of `blocks/chainSlice?fromHeight=<h>&toHeight=<h>`
///
/// `fromHeight=h` means > h when h < head
/// `fromHeight=h` means >= h when h = head
/// `toHeight=h` means <= h
///
/// Example for chain with heights a-b-c-d:
///   * a-c: returns headers [b, c]
///   * c-d: returns headers [d]
///   * c-z: returns headers [d]
///   * d-d: returns headers [d]
///   * d-z: returns headers [d]
async fn chain_slice(
    State(state): State<Arc<APIData>>,
    params: Query<ChainSliceParameters>,
) -> Json<Value> {
    // let params: ChainSliceParameters = params.0;
    assert!(params.from_height <= params.to_height);
    let max_height = state.heights.iter().max().unwrap().to_owned();
    let start_h = if params.from_height < max_height {
        params.from_height + 1
    } else {
        params.from_height
    };
    let end_h = std::cmp::min(params.to_height, max_height) + 1;
    let headers = (start_h..end_h)
        .map(|h| state.height_header_lookup[&h])
        .map(|i| state.blocks[i].to_json().get("header").unwrap().clone())
        .collect();
    Json(headers)
}

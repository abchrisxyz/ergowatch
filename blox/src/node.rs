mod api;
pub mod models;

use api::NodeAPI;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Node is unreachable. Could be a tempory outage but make sure your config is set correctly and the node is running.")]
    NodeUnreachable,
    #[error("Bad node API request: {0}")]
    API400BadRequest(String),
    #[error("Node API request not found: {0}")]
    API404Notfound(String),
    #[error("Error while requesting ({0})")]
    APIError(String),
    #[error("Failed parsing response from node")]
    DeserializationError,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub id: String,
    pub api: NodeAPI,
}

impl Node {
    pub fn new(id: &str, url: &str) -> Self {
        Self {
            id: String::from(id),
            api: NodeAPI::new(url),
        }
    }
}

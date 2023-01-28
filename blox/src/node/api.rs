use super::models::Block;
use super::models::Header;
use super::models::HeaderID;
use super::models::Height;
use super::models::NodeInfo;
use super::NodeError;
use reqwest;
use reqwest::StatusCode;

#[derive(Debug, Clone)]
pub struct NodeAPI {
    url: String,
    qry_info: String,
}

impl NodeAPI {
    pub fn new(url: &str) -> Self {
        tracing::event!(tracing::Level::INFO, url);
        Self {
            url: String::from(url),
            qry_info: format!("{}/info", url),
        }
    }

    /// Get current node info (trimmed down version)
    pub async fn info(&self) -> Result<NodeInfo, NodeError> {
        let response = self.get(&self.qry_info).await?;
        let node_info: NodeInfo = response.json().await.unwrap();
        Ok(node_info)
    }

    /// Get header ID's of blocks at given `height`
    pub async fn blocks_at(&self, height: Height) -> Result<Vec<String>, NodeError> {
        let url = format!("{}/blocks/at/{}", self.url, height);
        let response = self.get(&url).await?;
        let header_ids: Vec<String> = response.json().await.unwrap();
        Ok(header_ids.to_owned())
    }

    /// Get full block from `header_id`
    pub async fn block(&self, header_id: &HeaderID) -> Result<Block, NodeError> {
        let url = format!("{}/blocks/{}", self.url, header_id);
        let response = self.get(&url).await?;
        response
            .json()
            .await
            .map_err(|_| NodeError::DeserializationError)
    }

    /// Get full header from `header_id`
    pub async fn header(&self, header_id: &HeaderID) -> Result<Header, NodeError> {
        let url = format!("{}/blocks/{}/header", self.url, header_id);
        let response = self.get(&url).await?;
        response
            .json()
            .await
            .map_err(|_| NodeError::DeserializationError)
    }

    /// Get headers in a specified range
    ///
    ///
    pub async fn chainslice(&self, from_h: i32, to_h: i32) -> Result<Vec<Header>, NodeError> {
        let url = format!(
            "{}/blocks/chainSlice?fromHeight={}&toHeight={}",
            self.url, from_h, to_h
        );
        let response = self.get(&url).await?;
        response
            .json()
            .await
            .map_err(|_| NodeError::DeserializationError)
    }
}

impl NodeAPI {
    /// Send a GET request
    async fn get(&self, url: &str) -> Result<reqwest::Response, NodeError> {
        // tracing::event!(tracing::Level::INFO, url);
        let response = reqwest::get(url)
            .await
            .map_err(|_| NodeError::NodeUnreachable)?;

        match response.status() {
            StatusCode::OK => Ok(response),
            StatusCode::BAD_REQUEST => Err(NodeError::API400BadRequest(url.to_string())),
            StatusCode::NOT_FOUND => Err(NodeError::API404Notfound(url.to_string())),
            _ => Err(NodeError::APIError(url.to_string())),
        }
    }
}

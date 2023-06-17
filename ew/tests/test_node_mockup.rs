// cargo test --test '*' -- --test-threads=1
mod common;

use pretty_assertions::assert_eq;
use tokio;

use common::blocks::TestBlock;
use common::node_mockup::TestNode;
use ew::core::testing::NodeBlock;
use ew::core::testing::NodeHeader;

#[tokio::test]
/// When chain is like so:
///
/// 1 - 2 - 3 - 4 - 5
///       \ 3bis
///
/// /blocks/at/3 should return header id of blocks 3 and 3bis
async fn test_blocks_at_height() {
    let block_ids = ["1", "2", "3bis*", "3", "4", "5"];
    let mock_node = TestNode::run(&block_ids).await;
    let url = format!("{}/blocks/at/3", mock_node.url());
    let response = reqwest::get(url).await.unwrap();
    assert!(response.status().is_success());
    let header_ids: Vec<String> = response.json().await.unwrap();
    assert_eq!(header_ids.len(), 2);
    assert_eq!(header_ids[0], TestBlock::from_id("3bis").header_id());
    assert_eq!(header_ids[1], TestBlock::from_id("3").header_id());
}

#[tokio::test]
async fn test_blocks_from_header_id() {
    let block_ids = ["1", "2", "3bis*", "3", "4", "5"];
    let mock_node = TestNode::run(&block_ids).await;
    let header_id = TestBlock::from_id("4").header_id().to_owned();
    let url = format!("{}/blocks/{header_id}", mock_node.url());
    let response = reqwest::get(url).await.unwrap();
    assert!(response.status().is_success());
    let block: NodeBlock = response.json().await.unwrap();
    assert_eq!(block.header.height, 4);
    assert_eq!(block.header.id, header_id);
}

#[tokio::test]
async fn test_header_from_header_id() {
    let block_ids = ["1", "2", "3bis*", "3", "4", "5"];
    let mock_node = TestNode::run(&block_ids).await;
    let header_id = TestBlock::from_id("4").header_id().to_owned();
    let url = format!("{}/blocks/{header_id}/header", mock_node.url());
    let response = reqwest::get(url).await.unwrap();
    assert!(response.status().is_success());
    let header: NodeHeader = response.json().await.unwrap();
    assert_eq!(header.height, 4);
    assert_eq!(header.id, header_id);
}

#[tokio::test]
async fn test_chain_slice() {
    // Note blocks are unordered, but returned headers should be
    let block_ids = ["1", "2", "4", "3*", "3bis", "5"];
    let mock_node = TestNode::run(&block_ids).await;
    let url = format!(
        "{}/blocks/chainSlice?&fromHeight=2&toHeight=4",
        mock_node.url()
    );
    let response = reqwest::get(url).await.unwrap();
    assert!(response.status().is_success());
    let headers: Vec<NodeHeader> = response.json().await.unwrap();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].id, TestBlock::from_id("3bis").header_id());
    assert_eq!(headers[1].id, TestBlock::from_id("4").header_id());
}

#[tokio::test]
async fn test_chain_slice_tail() {
    // when fromHeight = last height
    let block_ids = ["1", "2", "3bis", "3*", "4", "5"];
    let mock_node = TestNode::run(&block_ids).await;
    let url = format!(
        "{}/blocks/chainSlice?&fromHeight=5&toHeight=10",
        mock_node.url()
    );
    let response = reqwest::get(url).await.unwrap();
    assert!(response.status().is_success());
    let headers: Vec<NodeHeader> = response.json().await.unwrap();
    assert_eq!(headers.len(), 1);
    assert_eq!(headers[0].id, TestBlock::from_id("5").header_id());
}

use reqwest;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
use super::models::Block;
use super::models::HeaderID;
use super::models::Height;
use super::models::NodeInfo;

const NODE_URL: &str = "http://192.168.1.72:9053";

// pub fn get_node_height() -> Result<Height, reqwest::Error> {
//     let url = format!("{}/info", NODE_URL);
//     println!("URL: {}", url);
//     let node_info: NodeInfo = reqwest::blocking::get(url)?.json()?;
//     Ok(node_info.full_height)
// }

pub async fn get_node_height() -> Result<Height> {//, reqwest::Error> {
    let url = format!("{}/info", NODE_URL);
    println!("URL: {}", url);
    let node_info: NodeInfo = reqwest::get(url).await?.json().await?;
    Ok(node_info.full_height)
}

// pub fn get_block_at(height: Height) -> Result<HeaderID, reqwest::Error> {
//     let url = format!("{}/blocks/at/{}", NODE_URL, height);
//     println!("URL: {}", url);
//     let json: Vec<String> = reqwest::blocking::get(url)?.json()?;
//     assert_eq!(json.len(), 1);
//     Ok(json[0].to_owned())
// }

pub async fn get_block_at(height: Height) -> Result<HeaderID> {
    let url = format!("{}/blocks/at/{}", NODE_URL, height);
    println!("URL: {}", url);
    let json: Vec<String> = reqwest::get(url).await?.json().await?;
    assert_eq!(json.len(), 1);
    Ok(json[0].to_owned())
}

// pub fn get_block(header_id: HeaderID) -> Result<Block, reqwest::Error> {
//     let url = format!("{}/blocks/{}", NODE_URL, header_id);
//     println!("URL: {}", url);
//     let json: Block = reqwest::blocking::get(url)?.json()?;
//     Ok(json)
// }

pub async fn get_block(header_id: HeaderID) -> Result<Block> {
    let url = format!("{}/blocks/{}", NODE_URL, header_id);
    println!("URL: {}", url);
    let json: Block = reqwest::get(url).await?.json().await?;
    Ok(json)
}

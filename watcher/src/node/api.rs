use log::debug;
use reqwest;

use super::models::Block;
use super::models::HeaderID;
use super::models::Height;
use super::models::NodeInfo;

const NODE_URL: &str = "http://192.168.1.72:9053";

pub fn get_height() -> Result<Height, reqwest::Error> {
    let url = format!("{}/info", NODE_URL);
    debug!("URL: {}", url);
    let node_info: NodeInfo = reqwest::blocking::get(url)?.json()?;
    Ok(node_info.full_height)
}

pub fn get_block_at(height: Height) -> Result<HeaderID, reqwest::Error> {
    let url = format!("{}/blocks/at/{}", NODE_URL, height);
    debug!("URL: {}", url);
    let json: Vec<String> = reqwest::blocking::get(url)?.json()?;
    assert_eq!(json.len(), 1);
    Ok(json[0].to_owned())
}

pub fn get_block(header_id: HeaderID) -> Result<Block, reqwest::Error> {
    let url = format!("{}/blocks/{}", NODE_URL, header_id);
    debug!("URL: {}", url);
    let json: Block = reqwest::blocking::get(url)?.json()?;
    Ok(json)
}

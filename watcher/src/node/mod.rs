pub mod models;

use log::debug;
use reqwest;

use models::Block;
use models::HeaderID;
use models::Height;
use models::NodeInfo;


pub struct Node {
    pub url: String
}

impl Node {
    pub fn new() -> Self {
        Node {
            url: String::from("http://192.168.1.72:9053")
        }
    }

    pub fn get_height(&self) -> Result<Height, reqwest::Error> {
        let url = format!("{}/info", self.url);
        debug!("URL: {}", url);
        let node_info: NodeInfo = reqwest::blocking::get(url)?.json()?;
        Ok(node_info.full_height)
    }
    
    pub fn get_block_at(&self, height: Height) -> Result<HeaderID, reqwest::Error> {
        let url = format!("{}/blocks/at/{}", self.url, height);
        debug!("URL: {}", url);
        let json: Vec<String> = reqwest::blocking::get(url)?.json()?;
        assert_eq!(json.len(), 1);
        Ok(json[0].to_owned())
    }
    
    pub fn get_block(&self, header_id: HeaderID) -> Result<Block, reqwest::Error> {
        let url = format!("{}/blocks/{}", self.url, header_id);
        debug!("URL: {}", url);
        let json: Block = reqwest::blocking::get(url)?.json()?;
        Ok(json)
    }
}
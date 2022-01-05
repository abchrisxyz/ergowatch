mod node;

// pub struct BlockHeader {
//     pub height: u32,
//     pub id: String,
//     pub parent_id: Option<String>,
// }

// mod db {
//     use super::BlockHeader;
//     pub fn get_latest_header() -> Option<BlockHeader> {
//         todo!();
//     }
// }

#[tokio::main]
async fn main() {
    println!("Starting Ergo Watcher");

    // let db_header = db::get_latest_header();


    match node::api::get_block_at(500000).await {
        Ok(ip) => println!("Block ID: {}", ip),
        Err(error) => println!("Error: {}", error),
    }

    match node::api::get_node_height().await {
        Ok(info) => println!("Node info: {:?}", info),
        Err(error) => println!("Error: {}", error),
    }

    let header_id =
        // String::from("637ad491b3bb0762657029f076410e0afafba10f4d8d390db64d74d224e7af8b");
        String::from("18dbf34ce6a6934bb06a8598505d6fa4fccb46064c797c734814e20c438cc4f8");
    match node::api::get_block(header_id).await {
        // Ok(block) => println!("Block success!"),
        Ok(block) => {
            println!("Block success!");
            println!("Block: {:?}", block.header.id);
        }
        Err(error) => println!("Error: {}", error),
    }
}

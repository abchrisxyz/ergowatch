mod addresses;
mod blocks;
mod boxes;
mod meta;

use std::collections::HashMap;
use std::collections::HashSet;
use tokio_postgres::NoTls;

use super::ergo;
use super::node;
use super::types::Address;
use super::types::AddressID;
use super::types::Block;
use super::types::BoxID;
use super::types::CoreData;
use super::types::ErgoTree;
use super::types::Head;
use super::types::Height;
use super::types::Input;
use super::types::Output;
use super::types::Transaction;
use crate::config::PostgresConfig;
use crate::utils::Schema;

#[derive(Debug)]
pub(super) struct Store {
    client: tokio_postgres::Client,
    head: Head,
    last_address_id: i64,
}

impl Store {
    pub async fn new(pgconf: PostgresConfig) -> Self {
        tracing::debug!("initializing new store");
        let (mut client, connection) = tokio_postgres::connect(&pgconf.connection_uri, NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let schema = Schema::new("core", include_str!("store/schema.sql"));
        schema.init(&mut client).await;

        let head = blocks::last_head(&client).await;
        tracing::debug!("head: {:?}", &head);
        let last_address_id = addresses::get_max_id(&mut client).await;
        tracing::debug!(last_address_id);
        Self {
            client,
            head,
            last_address_id,
        }
    }

    pub(super) async fn include_genesis_boxes(&mut self, boxes: String) {
        tracing::info!("including genesis boxes");
        assert!(self.head.is_initial());
        self.head = Head::genesis();
        let pgtx = self.client.transaction().await.unwrap();

        // Store in dummy json block to be retrievable the same way as other boxes.
        let block = String::from(format!(
            r#"
            {{
                "header": {{
                    "height": 0,
                    "id": "0000000000000000000000000000000000000000000000000000000000000000",
                    "timestamp": 1561978800000
                }},
                "blockTransactions": {{
                    "transactions": [
                        {{
                            "outputs": {}
                        }}
                    ]
                }}
            }}"#,
            &boxes
        ));
        blocks::insert(&pgtx, 0, block).await;

        // Index boxes
        let node_boxes: Vec<node::models::Output> = serde_json::from_str(&boxes).unwrap();

        // manually insert box indices for genesis boxes (all have index zero, so enumerate)
        let box_indices = node_boxes
            .iter()
            .enumerate()
            .map(|(i, b)| boxes::BoxIndex {
                box_id: &b.box_id,
                height: 0,
                tx_index: 0,
                output_index: i as i32,
            })
            .collect();
        boxes::insert_many(&pgtx, box_indices).await;

        // Index addresses
        for output in node_boxes {
            let address = ergo::ergo_tree::base16_to_address(&output.ergo_tree);
            let spot_height = 0;
            self.last_address_id += 1;
            addresses::index_new(
                &pgtx,
                &addresses::AddressRecord::new(self.last_address_id, spot_height, address),
            )
            .await;
        }
        pgtx.commit().await.unwrap();
    }

    /// Inlude and expand block.
    ///
    /// Skips inclusion if block already processed.
    pub(super) async fn process(&mut self, height: Height, text_block: String) -> CoreData {
        // check block is child of parent and store json or skip if one of parents

        // Parse into node block
        let node_block: node::models::Block = serde_json::from_str(&text_block).unwrap();
        assert_eq!(height, node_block.header.height);

        // Check if block is new or already processed
        let is_next_block = height > self.head.height;
        if is_next_block {
            assert_eq!(height, self.head.height + 1);
            assert_eq!(node_block.header.parent_id, self.head.header_id);
        }
        tracing::debug!(is_next_block);

        let pgtx = self.client.transaction().await.unwrap();
        if is_next_block {
            blocks::insert(&pgtx, height, text_block).await;
            boxes::insert_many(&pgtx, collect_box_indices(&node_block)).await;
        }

        // Collect (data-)input box id's
        let input_box_ids = collect_input_ids(&node_block);
        // Retrieve corresponding UTxO data mapped by box_id.
        let input_boxes = boxes::map_boxes(&pgtx, input_box_ids).await;

        // Map input and output boxes' ergo trees to corresponding address id's
        let (max_address_id, address_ids) =
            compile_address_ids(&pgtx, &node_block, &input_boxes, self.last_address_id).await;
        self.last_address_id = max_address_id;

        pgtx.commit().await.unwrap();

        // Update head
        if is_next_block {
            self.head.height = node_block.header.height;
            self.head.header_id = node_block.header.id.clone();
        }

        // Convert intermediate input box representations to final input type
        let mut inputs: HashMap<BoxID, Input> = HashMap::new();
        for (box_id, utxo) in input_boxes.into_iter() {
            // let box_id = utxo.output.box_id.clone();
            let address_id = address_ids[&utxo.output.ergo_tree];
            let size = match ergo::boxes::calc_box_size(&utxo.output) {
                Some(s) => s,
                None => 0,
            };
            let input = Input {
                box_id: utxo.output.box_id,
                address_id: address_id,
                index: utxo.output.index,
                value: utxo.output.value,
                additional_registers: utxo.output.additional_registers,
                assets: utxo.output.assets,
                size: size,
                creation_height: utxo.height,
                creation_timestamp: utxo.timestamp,
            };
            inputs.insert(box_id, input);
        }

        // Convert to core block
        let core_block = Block {
            header: node_block.header.into(),
            transactions: node_block
                .block_transactions
                .transactions
                .into_iter()
                .enumerate()
                .map(|(i, tx)| Transaction {
                    id: tx.id,
                    index: i as i32,
                    outputs: tx
                        .outputs
                        .into_iter()
                        .map(|op| {
                            let address_id = address_ids[&op.ergo_tree];
                            Output::from_node_output(op, address_id)
                        })
                        .collect(),
                    inputs: tx
                        .inputs
                        .iter()
                        .map(|ip| inputs[&ip.box_id].clone())
                        .collect(),
                    data_inputs: tx
                        .data_inputs
                        .iter()
                        .map(|di| inputs[&di.box_id].clone())
                        .collect(),
                })
                .collect(),
            extension: node_block.extension,
            ad_proofs: node_block.ad_proofs,
            size: node_block.size,
        };

        CoreData { block: core_block }
    }

    /// Roll back block at given `head`.
    ///
    /// Must be the last included block.
    /// Return head representing previous block in store.
    pub(super) async fn roll_back(&mut self, head: &Head) -> Head {
        assert_eq!(&self.head, head);

        let pgtx = self.client.transaction().await.unwrap();

        // Delete block at height h
        blocks::delete(&pgtx, head.height).await;

        // Delete boxes registered ar height h
        boxes::delete_at(&pgtx, head.height).await;

        // Delete addresses spotted at height h
        addresses::delete_at(&pgtx, head.height).await;

        pgtx.commit().await.unwrap();

        // Retrieve previous head
        let prev_head = blocks::last_head(&self.client).await;

        // Decrement store head and return
        self.head = prev_head.clone();
        prev_head
    }

    /// Checks if store contains genesis boxes
    pub(super) async fn has_genesis_boxes(&self) -> bool {
        let qry = "
            select exists(
                select *
                from core.boxes
                where height = 0
            );";
        self.client.query_one(qry, &[]).await.unwrap().get(0)
    }

    pub(super) async fn get_genesis_boxes(&mut self) -> Vec<Output> {
        tracing::debug!("getting genesis boxes");
        //TODO: simplify - no need for db tx here but using one since called
        // helpers have no client version.
        let pgtx = self.client.transaction().await.unwrap();
        let node_boxes = boxes::get_genesis_boxes(&pgtx).await;
        // Genesis boxes are guaranteed to have been indexed in store already,
        // so we can just query their address id's and not worry about handling
        // any new addresses.
        let box_addys: Vec<Address> = node_boxes
            .iter()
            .map(|nb| ergo::ergo_tree::base16_to_address(&nb.ergo_tree))
            .collect();
        let mut address_ids: Vec<AddressID> = vec![];
        for address in &box_addys {
            let id = addresses::get_id(&pgtx, address).await;
            address_ids.push(id);
        }
        // Convert to core Outputs
        pgtx.rollback().await.unwrap();
        node_boxes
            .into_iter()
            .enumerate()
            .map(|(i, b)| Output::from_node_output(b, address_ids[i]))
            .collect()
    }

    async fn include(&mut self, h: Height, str_block: &str) {
        todo!()
    }
}

/// Extract box indices for all outputs in a node block
fn collect_box_indices(block: &node::models::Block) -> Vec<boxes::BoxIndex> {
    block
        .block_transactions
        .transactions
        .iter()
        .enumerate()
        .flat_map(|(itx, tx)| {
            tx.outputs.iter().map(move |output| boxes::BoxIndex {
                box_id: &output.box_id,
                height: block.header.height,
                tx_index: itx as i32,
                output_index: output.index,
            })
        })
        .collect()
}

/// Extracts all input and data-input box id's from `block`.
fn collect_input_ids(block: &node::models::Block) -> HashSet<BoxID> {
    let mut box_ids = HashSet::new();
    for tx in &block.block_transactions.transactions {
        for ip in &tx.inputs {
            box_ids.insert(ip.box_id.clone());
        }
        for di in &tx.data_inputs {
            box_ids.insert(di.box_id.clone());
        }
    }
    box_ids
}

/// Maps all of a block's ergo trees to address id's.
///
/// Indexes any new addresses found in `node_block` outputs.
/// Returns new highest address id and a tree-to-address-id map.
async fn compile_address_ids(
    pgtx: &tokio_postgres::Transaction<'_>,
    node_block: &node::models::Block,
    input_boxes: &HashMap<BoxID, super::store::boxes::UTxO>,
    last_address_id: AddressID,
) -> (AddressID, HashMap<ErgoTree, AddressID>) {
    // Map to be populated and returned
    let mut ids: HashMap<ErgoTree, AddressID> = HashMap::new();

    // Keeps track of last address_id
    let mut max_address_id = last_address_id;

    // Going over each tx in turn to handle outputs spent in same block.
    // This allows assuming all inputs will have an allocated address id already.
    for tx in &node_block.block_transactions.transactions {
        // tracing::debug!("tx: {}", tx.id);
        // Inputs and data-inputs have all been registered so just retrieve their address id
        for input in &tx.inputs {
            // tracing::debug!("input box_id: {}", input.box_id);
            let utxo = &input_boxes[&input.box_id];
            let address = ergo::ergo_tree::base16_to_address(&utxo.output.ergo_tree);
            if !ids.contains_key(&address) {
                let address_id = addresses::get_id(&pgtx, &address).await;
                ids.insert(utxo.output.ergo_tree.clone(), address_id);
            }
        }

        // Outputs will contain some new addresses
        for output in &tx.outputs {
            if ids.contains_key(&output.ergo_tree) {
                continue;
            }
            let address = ergo::ergo_tree::base16_to_address(&output.ergo_tree);
            let address_id = match addresses::get_id_opt(&pgtx, &address).await {
                Some(id) => id,
                None => {
                    let spot_height = node_block.header.height;
                    max_address_id += 1;
                    addresses::index_new(
                        &pgtx,
                        &addresses::AddressRecord::new(max_address_id, spot_height, address),
                    )
                    .await;
                    max_address_id
                }
            };
            ids.insert(output.ergo_tree.clone(), address_id);
        }
    }
    tracing::debug!(
        "compiled {} addresses - new max id: {}",
        ids.len(),
        max_address_id
    );
    (max_address_id, ids)
}

mod addresses;
mod boxes;
mod headers;
mod meta;
mod tokens;

use lru::LruCache;
use std::collections::HashMap;
use tokio_postgres::NoTls;

use super::ergo;
use super::node;
use super::types::AddressID;
use super::types::Asset;
use super::types::AssetID;
use super::types::Block;
use super::types::BoxData;
use super::types::BoxID;
use super::types::CoreData;
use super::types::Header;
use super::types::Height;
use super::types::Registers;
use super::types::TokenID;
use super::types::Transaction;
use crate::config::PostgresConfig;
use crate::utils::Schema;

#[derive(Debug)]
pub(super) struct Store {
    client: tokio_postgres::Client,
    header: Header,
    address_cache: AddressCache,
    asset_cache: AssetCache,
}

#[derive(Debug)]
/// Cached data to speed up ergo tree to address id conversion.
struct AddressCache {
    /// Keep track of highest address id
    pub address_count: i64,
    /// Maps ergo trees to an address id (global index)
    pub lru: LruCache<String, AddressID>,
}

impl AddressCache {
    pub fn new(last_address_id: AddressID) -> Self {
        Self {
            address_count: last_address_id.sequence_position(),
            lru: LruCache::new(std::num::NonZeroUsize::new(5000).unwrap()),
        }
    }

    /// Resets the cache by clearing all entries and setting `last_id` to `last_address_id`.
    pub fn reset(&mut self, last_address_id: AddressID) {
        self.lru.clear();
        self.address_count = last_address_id.sequence_position();
    }
}

#[derive(Debug)]
/// Cached asset id's to replace Digest32 token id's.
struct AssetCache {
    /// Keep track of highest address id
    pub last_id: i64,
    /// Maps Digest32 token id's corresponfing asset id.
    pub lru: LruCache<TokenID, AssetID>,
}

impl AssetCache {
    pub fn new(last_asset_id: AssetID) -> Self {
        Self {
            last_id: last_asset_id,
            lru: LruCache::new(std::num::NonZeroUsize::new(5000).unwrap()),
        }
    }

    /// Resets the cache by clearing all entries and setting `last_id` to `last_asset_id`.
    pub fn reset(&mut self, last_asset_id: AssetID) {
        self.lru.clear();
        self.last_id = last_asset_id;
    }
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

        let header = headers::get_last_main(&client)
            .await
            .unwrap_or(Header::initial());
        tracing::debug!("current header: {:?}", &header);
        let last_address_id = addresses::get_max_id(&mut client).await;
        let last_asset_id = tokens::get_max_id(&mut client).await;

        Self {
            client,
            header,
            address_cache: AddressCache::new(last_address_id),
            asset_cache: AssetCache::new(last_asset_id),
        }
    }

    pub(super) fn header(&self) -> &Header {
        &self.header
    }

    pub(super) async fn contains_header(&self, header: &Header) -> bool {
        headers::exists_and_is_main_chain(&self.client, header).await
    }

    pub(super) async fn include_genesis_boxes(&mut self, boxes: String) {
        tracing::info!("including genesis boxes");
        assert!(self.header.is_initial());
        self.header = Header::genesis();
        let height = self.header.height;

        let pgtx = self.client.transaction().await.unwrap();

        // Index dummy header for genesis
        headers::insert_main(&pgtx, &self.header).await;

        // Index genesis boxes
        let node_boxes: Vec<node::models::Output> = serde_json::from_str(&boxes).unwrap();
        let mut box_records: Vec<boxes::BoxRecord> = vec![];
        for op in &node_boxes {
            let address_id =
                map_address_id(&pgtx, &op.ergo_tree, height, &mut self.address_cache).await;
            box_records.push(boxes::BoxRecord {
                box_id: &op.box_id,
                height,
                creation_height: op.creation_height,
                address_id,
                value: op.value,
                size: ergo::boxes::calc_box_size(&op).unwrap(),
                assets: map_asset_ids(&pgtx, &op.assets, height, &mut self.asset_cache).await,
                registers: &op.additional_registers,
            });
        }
        boxes::insert_many(&pgtx, &box_records).await;

        pgtx.commit().await.unwrap();
    }

    /// Include and expand block.
    ///
    /// Skips inclusion if block already processed.
    pub(super) async fn process(&mut self, node_block: node::models::Block) -> CoreData {
        /*

        if next block:
            index new outputs
                index new addresses
                index new tokens

        collect boxes

        retrieve input and output boxes
        build core block

        ------
        What needs to happen:

        If new block:
            - Index headers
            - Index node outputs
                - replace ergo_tree by address_id
                - replace asset token_ids by token_gids
                - compute box size
            - Convert to core outputs

        If existing block:
            - Load outputs

        - Load inputs
        - Prepare core block




        for tx in txs:
            for box_id in inputs:
                inputs[box_id] = fetch_input(box_id)
            for node_output in outputs:
                outputs[box_id] =

        fetch_input(box_id):
            if in box_cache, take
            else fetch from db, don't add to cache


        */

        // Check if block is new or already processed.
        // If new, ensure it is a child of current tip.
        let is_next_block = node_block.header.height > self.header.height;
        if is_next_block {
            assert_eq!(node_block.header.height, self.header.height + 1);
            assert_eq!(node_block.header.parent_id, self.header.header_id);
        }

        // Wrap everyting in one db transaction
        let pgtx = self.client.transaction().await.unwrap();

        // Update head
        if is_next_block {
            self.header = Header {
                height: node_block.header.height,
                timestamp: node_block.header.timestamp,
                header_id: node_block.header.id.clone(),
                parent_id: node_block.header.parent_id.clone(),
            };
            headers::insert_main(&pgtx, &self.header).await;
        }

        // Index/load outputs
        let mut outputs: HashMap<BoxID, BoxData> = if is_next_block {
            index_outputs(
                &pgtx,
                &node_block,
                &mut self.address_cache,
                &mut self.asset_cache,
            )
            .await
        } else {
            let output_box_ids: Vec<&BoxID> = node_block
                .block_transactions
                .transactions
                .iter()
                .flat_map(|tx| tx.outputs.iter().map(|op| &op.box_id))
                .collect();
            boxes::map_boxes(&pgtx, output_box_ids).await
        };

        // Load data-inputs
        let data_input_box_ids: Vec<&BoxID> = node_block
            .block_transactions
            .transactions
            .iter()
            .flat_map(|tx| tx.data_inputs.iter().map(|di| &di.box_id))
            .collect();
        let data_inputs: HashMap<BoxID, BoxData> =
            boxes::map_boxes(&pgtx, data_input_box_ids).await;

        // Load inputs
        // TODO: consider finding some inputs in the outputs (e.g. fee boxes)
        // TODO: consider caching recent outputs (e.g. emission contract)
        let input_box_ids: Vec<&BoxID> = node_block
            .block_transactions
            .transactions
            .iter()
            .flat_map(|tx| tx.inputs.iter().map(|ip| &ip.box_id))
            .collect();
        let mut inputs: HashMap<BoxID, BoxData> = boxes::map_boxes(&pgtx, input_box_ids).await;

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
                        .map(|op| outputs.remove(&op.box_id).unwrap())
                        .collect(),
                    // Data inputs first, because inputs will remove entries from the cache
                    data_inputs: tx
                        .data_inputs
                        .iter()
                        .map(|di| data_inputs.get(&di.box_id).cloned().unwrap())
                        .collect(),
                    inputs: tx
                        .inputs
                        .iter()
                        .map(|ip| inputs.remove(&ip.box_id).unwrap())
                        .collect(),
                })
                .collect(),
            extension: node_block.extension,
            // ad_proofs: node_block.ad_proofs,
            size: node_block.size,
        };

        pgtx.commit().await.unwrap();

        CoreData { block: core_block }
    }

    /// Roll back block with given `header`.
    ///
    /// Must be the last included block.
    /// Returna headwe representing previous block in store.
    pub(super) async fn roll_back(&mut self, header: &Header) -> Header {
        assert_eq!(&self.header, header);

        let pgtx = self.client.transaction().await.unwrap();

        // Delete main chain header at height h
        headers::delete_main_at(&pgtx, header.height).await;

        // Delete boxes registered ar height h
        boxes::delete_at(&pgtx, header.height).await;

        // Delete addresses spotted at height h
        let n_deleted = addresses::delete_at(&pgtx, header.height).await;
        // Reset the cache if there where any new ones in rolled back block.
        // A bit radical, but not taking any risks. This will only affect synced
        // cursors anyway, so block processing time is less of an issue.
        if n_deleted > 0 {
            self.address_cache.reset(addresses::get_max_id(&pgtx).await)
        }

        // Delete tokens spotted at height h
        let n_deleted = tokens::delete_at(&pgtx, header.height).await;
        // Reset the cache if there where any new ones in rolled back block.
        // Same comments as for address cache above.
        if n_deleted > 0 {
            self.asset_cache.reset(tokens::get_max_id(&pgtx).await)
        }

        pgtx.commit().await.unwrap();

        // Retrieve previous header
        let prev_header = headers::get_last_main(&self.client)
            .await
            .expect("Rollback implies previous headers");

        // Decrement store head and return
        self.header = prev_header.clone();
        prev_header
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

    pub(super) async fn get_genesis_boxes(&mut self) -> Vec<BoxData> {
        tracing::debug!("getting genesis boxes");
        boxes::get_genesis_boxes(&self.client).await
    }
}

/// Saves boxes created in given `node_block` to db and adds entries to the block_cache.
///
/// Takes care of
/// - calculating box size
/// - assigning an address id
/// - replacing asset token id's with gid's
async fn index_outputs(
    pgtx: &tokio_postgres::Transaction<'_>,
    node_block: &node::models::Block,
    address_cache: &mut AddressCache,
    asset_cache: &mut AssetCache,
) -> HashMap<BoxID, BoxData> {
    let height = node_block.header.height;
    let mut box_records: Vec<boxes::BoxRecord> = vec![];
    for tx in &node_block.block_transactions.transactions {
        for op in &tx.outputs {
            let address_id = map_address_id(pgtx, &op.ergo_tree, height, address_cache).await;
            let assets = map_asset_ids(pgtx, &op.assets, height, asset_cache).await;
            let size = match ergo::boxes::calc_box_size(&op) {
                Some(s) => s,
                // Current calculation of box size can fail when hitting
                // undeserializable boxes. Using a default size of 1 here.
                // While unrealistic, it allows to look such boxes up and
                // maybe update them once we have a workaround.
                None => 1,
            };
            // Create box record
            box_records.push(boxes::BoxRecord {
                box_id: &op.box_id,
                height,
                creation_height: op.creation_height,
                address_id,
                value: op.value,
                size,
                assets,
                registers: &op.additional_registers,
            });
        }
    }
    // Store records
    boxes::insert_many(&pgtx, &box_records).await;

    // Convert box records to box data and return hashmap
    let mut map = HashMap::new();
    for r in box_records {
        let box_data = BoxData {
            box_id: r.box_id.clone(),
            creation_height: r.creation_height,
            address_id: r.address_id,
            value: r.value,
            additional_registers: Registers::new(r.registers.clone()),
            assets: match r.assets {
                Some(assets) => assets,
                None => vec![],
            },
            size: r.size,
            output_timestamp: node_block.header.timestamp,
        };
        map.insert(box_data.box_id.clone(), box_data);
    }
    map
}

/// Return an address id for the given `ergo_tree`.
///
/// Handles indexing of new trees/addresses.
///
/// * `pgtx` - A db transaction.
/// * `ergo_tree` - The ergo tree for which to return an address id.
/// * `spot_height` - Height of current block (used when indexing new addresses).
async fn map_address_id(
    pgtx: &tokio_postgres::Transaction<'_>,
    ergo_tree: &String,
    spot_height: Height,
    cache: &mut AddressCache,
) -> AddressID {
    // Try the cache first.
    match cache.lru.get(ergo_tree) {
        // Sweet, found it in the cache
        Some(id) => *id,
        // Not in the cache
        None => {
            // See if we can find it in the store.
            let address = ergo::ergo_tree::base16_to_address(ergo_tree);
            let uncached_id = match addresses::get_id_opt(&pgtx, &address).await {
                // Address was in store already
                Some(id) => id,
                // This is a new address - assign new id and index
                None => {
                    cache.address_count += 1;
                    let address_id = AddressID::new(cache.address_count, &address);
                    addresses::index_new(
                        &pgtx,
                        &addresses::AddressRecord::new(address_id, spot_height, address),
                    )
                    .await;
                    address_id
                }
            };
            // Cache and return
            cache.lru.put(ergo_tree.clone(), uncached_id);
            uncached_id
        }
    }
}

/// Converts token id's (Digest32) to gid's (i64).
///
/// Handles indexing of new tokens.
///
/// * `pgtx` - A db transaction.
/// * `node_assets` - Original assets as returned by the node api.
/// * `spot_height` - Height of current block (used when indexing new tokens).
async fn map_asset_ids(
    pgtx: &tokio_postgres::Transaction<'_>,
    node_assets: &Vec<node::models::Asset>,
    spot_height: Height,
    cache: &mut AssetCache,
) -> Option<Vec<Asset>> {
    if node_assets.is_empty() {
        return None;
    }
    let mut assets: Vec<Asset> = Vec::with_capacity(node_assets.len());
    for node_asset in node_assets {
        // Try the cache first.
        let asset_id = match cache.lru.get(&node_asset.token_id) {
            // Sweet, fount it in the cache
            Some(id) => *id,
            // Not in the cache
            None => {
                // See if we can find it in the store.
                let token_id = node_asset.token_id.clone();
                let uncached_id = match tokens::get_id_opt(&pgtx, &token_id).await {
                    // Token was in store already
                    Some(id) => id,
                    None => {
                        // This is a new token - assign new id and index
                        cache.last_id += 1;
                        tokens::index_new(
                            &pgtx,
                            &tokens::TokenRecord::new(cache.last_id, spot_height, token_id.clone()),
                        )
                        .await;
                        cache.last_id
                    }
                };
                // Cache and return
                cache.lru.put(token_id, uncached_id);
                uncached_id
            }
        };
        assets.push(Asset {
            asset_id,
            amount: node_asset.amount,
        });
    }
    Some(assets)
}

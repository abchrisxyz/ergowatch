use std::collections::HashMap;
use std::collections::HashSet;
use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::core::types::AddressID;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::core::types::NanoERG;
use crate::utils::Schema;

use super::parsing::Bal;
use super::parsing::Balance;
use super::parsing::ParserCache;
use super::types::BalanceRecord;
use super::Batch;
use crate::core::types::Timestamp;

mod balances;
mod composition;
mod counts;
mod diffs;
mod headers;

pub struct Store {
    client: tokio_postgres::Client,
    head: Head,
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

        let schema = Schema::new("erg", include_str!("store/schema.sql"));
        schema.init(&mut client).await;

        let head = headers::get_last(&client).await.head();
        tracing::debug!("head: {:?}", &head);

        Self { client, head }
    }

    pub(super) fn get_head(&self) -> &Head {
        &self.head
    }

    pub(super) async fn load_parser_cache(&self) -> ParserCache {
        ParserCache {
            last_address_counts: counts::get_last(&self.client).await,
            last_supply_composition: composition::get_last(&self.client).await,
        }
    }

    pub(super) async fn persist(&mut self, batch: Batch) {
        // tracing::debug!("persisting data for block {}", batch.header.height);
        let pgtx = self.client.transaction().await.unwrap();

        headers::insert(&pgtx, &batch.header).await;
        diffs::insert_many(&pgtx, &batch.diff_records).await;
        balances::upsert_many(&pgtx, &batch.balance_records).await;
        balances::delete_many(&pgtx, &batch.spent_addresses).await;
        counts::insert(&pgtx, &batch.address_counts).await;
        composition::insert(&pgtx, &batch.supply_composition).await;

        pgtx.commit().await.unwrap();

        // Update head
        self.head = batch.header.head();
    }

    /// Roll back changes from last block.
    pub(super) async fn roll_back(&mut self, height: Height) {
        tracing::debug!("rolling back block {}", height);
        assert_eq!(self.head.height, height);

        // Prepare rollback data for balances
        let brc: BalanceRollbackChanges = prepare_balance_rollback(&self.client, height).await;

        let pgtx = self.client.transaction().await.unwrap();

        headers::delete_at(&pgtx, height).await;
        diffs::delete_at(&pgtx, height).await;
        balances::upsert_many(&pgtx, &brc.upserts).await;
        balances::delete_many(&pgtx, &brc.deletes).await;
        headers::delete_at(&pgtx, height).await;
        counts::delete_at(&pgtx, height).await;
        composition::delete_at(&pgtx, height).await;

        pgtx.commit().await.unwrap();

        // Reload head
        let header = headers::get_last(&self.client).await;
        self.head = Head {
            height: header.height,
            header_id: header.id,
        }
    }

    /// Retrieve and map balance records for given address id's.
    ///
    /// Does not inlcude zero balances.
    pub(super) async fn map_balance_records(
        &self,
        address_ids: Vec<AddressID>,
    ) -> HashMap<AddressID, BalanceRecord> {
        // TODO: cache
        let recs = balances::get_many(&self.client, &address_ids).await;
        let mut map = HashMap::new();
        for r in recs {
            map.insert(r.address_id, r);
        }
        map
    }
}

/// Reperesents changes required to roll back balances.
struct BalanceRollbackChanges {
    /// Balance records to be upserted.
    /// Represent modified or deleted balances.
    pub upserts: Vec<BalanceRecord>,
    /// Address IDs of balances to be deleted.
    /// Represent balances created in rolled back block.
    pub deletes: Vec<AddressID>,
}

/// Derives changes needed to roll back balances.
///
/// * `client`: read-only db client
/// * `height`: height of block getting rolled back
///     
/// Doesn't apply any changes - only reads from db.
async fn prepare_balance_rollback(
    client: &tokio_postgres::Client,
    height: Height,
) -> BalanceRollbackChanges {
    // Get last header to retrieve rolled back block timestamp
    let header = headers::get_last(client).await;
    assert_eq!(header.height, height);
    let timestamp = header.timestamp;

    // Retrieve diff records in rolled back block.
    let diff_records = diffs::select_at(client, height).await;

    // Aggregate diffs by address
    let mut diff_lookup: HashMap<AddressID, NanoERG> = HashMap::new();
    for rec in &diff_records {
        // Sum diffs by address
        diff_lookup
            .entry(rec.address_id)
            .and_modify(|e| *e += rec.nano)
            .or_insert(rec.nano);
    }

    // Remove zero diffs from lookup (balances not affected)
    diff_lookup.retain(|_, v| *v != 0);

    // Retrieve current balances for diffed addresses
    let diff_addys: Vec<AddressID> = diff_lookup.keys().into_iter().cloned().collect();
    let balance_records = balances::get_many(client, &diff_addys).await;

    // Convert diff addresses to a HashSet
    let diff_addys: HashSet<AddressID> = diff_addys.into_iter().collect();

    // Collect balance addresses into a HashSet
    let balance_addys: HashSet<AddressID> =
        balance_records.iter().map(|br| br.address_id).collect();

    let mut brc = BalanceRollbackChanges {
        upserts: vec![],
        deletes: vec![],
    };

    // Reverse current balances
    for br in balance_records {
        // Any address with a balance record will also have a diff
        let nano = *diff_lookup.get(&br.address_id).unwrap();
        let reversed_bal = Bal::from(&br).reverse(nano, timestamp);
        match reversed_bal {
            // Balance was zero before the block, so remove from balances to roll back.
            Bal::Spent => brc.deletes.push(br.address_id),
            // Balance was non-zero before the block, so update to roll back
            Bal::Unspent(ubal) => brc.upserts.push(BalanceRecord::new(
                br.address_id,
                ubal.nano,
                ubal.mean_age_timestamp,
            )),
        }
    }

    // Spot addresses spent in rolled back block.
    // Those are addresses with a diff, but no balance anymore.
    let spent_addys: Vec<AddressID> = diff_addys
        .difference(&balance_addys)
        .into_iter()
        .cloned()
        .collect();

    // Recalculate balance and age from previous diff records
    for spent_address_id in spent_addys {
        let timestamped_diffs = diffs::get_address_diffs(client, spent_address_id).await;
        let bal = timestamped_diffs
            .iter()
            .take(timestamped_diffs.len() - 1) // don't include last diff from current block
            .fold(Bal::Spent, |acc, (amount, timestamp)| {
                acc.accrue(*amount, *timestamp)
            });
        match bal {
            // Should not happen
            Bal::Spent => panic!("Got zero balance while restoring spent address in roll back"),
            // Add to upserts to insert balance record back
            Bal::Unspent(ubal) => brc.upserts.push(BalanceRecord::new(
                spent_address_id,
                ubal.nano,
                ubal.mean_age_timestamp,
            )),
        }
    }

    brc
}

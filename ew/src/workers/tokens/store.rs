mod balances;
mod diffs;

use async_trait::async_trait;
use std::collections::HashMap;

use tokio_postgres::Transaction;

use super::types::AddressAsset;
use super::types::BalanceRecord;
use crate::constants::settings::ROLLBACK_HORIZON;
use crate::core::types::AddressID;
use crate::core::types::AssetID;
use crate::core::types::Header;
use crate::framework::store::BatchStore;
use crate::framework::store::PgStore;
use crate::framework::store::Revision;
use crate::framework::store::StoreDef;
use crate::framework::StampedData;

use super::types::Batch;
use super::WORKER_ID;

pub(super) const SCHEMA: StoreDef = StoreDef {
    schema_name: "tokens",
    worker_id: WORKER_ID,
    sql: include_str!("store/schema.sql"),
    revision: &Revision { major: 1, minor: 0 },
};

pub(super) type Store = PgStore<InnerStore>;
pub(super) struct InnerStore;

#[async_trait]
impl BatchStore for InnerStore {
    type B = Batch;

    async fn new() -> Self {
        Self {}
    }

    async fn persist(&mut self, pgtx: &Transaction<'_>, stamped_batch: &StampedData<Self::B>) {
        let batch = &stamped_batch.data;

        // Before modifying any balances, log current state to allow rollbacks.
        let height = stamped_batch.height;
        let new_address_assets = batch
            .balance_records
            .iter()
            .filter(|r| r.mean_age_timestamp == stamped_batch.timestamp)
            .map(|r| AddressAsset::new(r.address_id, r.asset_id))
            .collect();
        let modified_addresses = batch
            .balance_records
            .iter()
            .filter(|r| r.mean_age_timestamp != stamped_batch.timestamp)
            .map(|r| AddressAsset::new(r.address_id, r.asset_id))
            .collect();
        // Log addresses that will get created
        balances::logs::log_new_balances(pgtx, height, &new_address_assets).await;
        // Log current balance of addresses that will get modified
        balances::logs::log_existing_balances(pgtx, height, &modified_addresses).await;
        // Log current balance of addresses that will get spent
        balances::logs::log_existing_balances(pgtx, height, &batch.spent_addresses).await;
        // Delete old logs
        balances::logs::delete_logs_prior_to(pgtx, height - ROLLBACK_HORIZON).await;

        diffs::insert_many(&pgtx, &batch.diff_records).await;
        balances::upsert_many(&pgtx, &batch.balance_records).await;
        balances::delete_many(&pgtx, &batch.spent_addresses).await;
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        tracing::debug!("rolling back block {}", header.height);

        let height = header.height;
        diffs::delete_at(&pgtx, height).await;
        balances::upsert_many(&pgtx, &balances::logs::get_balances_at(pgtx, height).await).await;
        balances::delete_many(
            &pgtx,
            &balances::logs::get_address_assets_created_at(pgtx, height).await,
        )
        .await;
        balances::logs::delete_logs_at(pgtx, height).await;
    }
}

impl Store {
    /// Retrieve and map balance records for given address/asset pairs.
    ///
    /// Does not inlcude zero balances.
    pub(super) async fn map_balance_records(
        &self,
        address_assets: Vec<AddressAsset>,
    ) -> HashMap<(AddressID, AssetID), BalanceRecord> {
        // TODO: cache
        let recs = balances::get_many(self.get_client(), &address_assets).await;
        let mut map = HashMap::new();
        for r in recs {
            map.insert((r.address_id, r.asset_id), r);
        }
        map
    }
}

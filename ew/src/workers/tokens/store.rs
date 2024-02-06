mod balances;
mod diffs;

use async_trait::async_trait;
use std::collections::HashMap;

use tokio_postgres::Transaction;

use super::types::AddressAsset;
use super::types::BalanceRecord;
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

        diffs::insert_many(&pgtx, &batch.diff_records).await;
        balances::upsert_many(&pgtx, &batch.balance_records).await;
        balances::delete_many(&pgtx, &batch.spent_addresses).await;
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        tracing::debug!("rolling back block {}", header.height);

        let height = header.height;

        // Collect diffed Address/Assets
        let diffed: Vec<AddressAsset> = diffs::get_many_at(pgtx, height)
            .await
            .iter()
            .map(|dr| AddressAsset::new(dr.address_id, dr.asset_id))
            .collect();

        // Delete at
        diffs::delete_at(&pgtx, height).await;

        // Get previous balances for diffed address/assets
        let diffed_bals = diffs::get_non_zero_balances_for(pgtx, &diffed).await;

        // Delete all diffed address assets.
        // Alternative would be to delete only those that have no match in recalculated
        // balances (diffed_bals). This is simpler, even if less efficient.
        balances::delete_many(&pgtx, &diffed).await;

        // Re-insert non-zero balances
        balances::upsert_many(&pgtx, &diffed_bals).await;
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

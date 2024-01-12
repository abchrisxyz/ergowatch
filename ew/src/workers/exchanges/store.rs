mod deposit_addresses;
mod deposit_conflicts;
mod deposit_ignored;
mod main_addresses;
mod supply;

use async_trait::async_trait;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::AddressID;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::store::BatchStore;
use crate::framework::store::PatchableStore;
use crate::framework::store::PgStore;
use crate::framework::store::Revision;
use crate::framework::store::StoreDef;
use crate::framework::StampedData;

use super::parsing::ParserCache;
use super::types::Batch;
use super::types::DepositAddressConflictRecord;
use super::types::DepositAddressRecord;
use super::types::SupplyDiff;
use super::types::SupplyRecord;
use super::WORKER_ID;

pub(super) const SCHEMA: StoreDef = StoreDef {
    schema_name: "exchanges",
    worker_id: WORKER_ID,
    sql: include_str!("store/schema.sql"),
    revision: &Revision { major: 1, minor: 0 },
};

pub(super) type Store = PgStore<SpecStore>;
pub(super) struct SpecStore {
    pub rollback_patch: Option<Vec<SupplyDiff>>,
}

#[async_trait]
impl BatchStore for SpecStore {
    type B = Batch;

    async fn new() -> Self {
        Self {
            rollback_patch: None,
        }
    }

    async fn persist(&mut self, pgtx: &Transaction<'_>, stamped_batch: &StampedData<Self::B>) {
        let batch = &stamped_batch.data;

        // Insert new record *before* applying patch
        supply::insert(pgtx, &batch.supply).await;
        // Apply patch *after* inserting new record
        if !batch.supply_patch.is_empty() {
            supply::patch_deposits(pgtx, &batch.supply_patch).await;
        }

        // New deposit addresses
        deposit_addresses::insert_many(pgtx, &batch.deposit_addresses).await;

        // Deposit conflicts
        for conflict in &batch.deposit_conflicts {
            let address_spot_height = deposit_addresses::get_one(pgtx, conflict.address_id)
                .await
                .spot_height;
            deposit_addresses::delete_one(pgtx, conflict.address_id).await;
            deposit_conflicts::insert(pgtx, &conflict.to_record(address_spot_height)).await;
        }
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        tracing::debug!("rolling back block {}", header.height);

        // for any deposits spotted at h --> query supply diffs
        // same for conflicts spotted at h
        // apply inverse patch.
        // not an issue if upstream rolled back even more rollback events will
        // eventually make it to this worker and delete any stale records.

        // Delete supply record of rolled back block
        supply::delete_at(pgtx, header.height).await;

        // Delete deposit records spotted in rolled back block
        deposit_addresses::delete_spotted_at(pgtx, header.height).await;

        // Restore deleted conflicts as deposits (if their address spot height < than h)
        let deposits_to_restore: Vec<DepositAddressRecord> =
            deposit_conflicts::get_conflicted_at(pgtx, header.height)
                .await
                .into_iter()
                // ignore intra-block conflicts
                .filter(|c| c.deposit_spot_height != header.height)
                .map(|c| c.into())
                .collect();
        deposit_addresses::insert_many(pgtx, &deposits_to_restore).await;

        // Delete conflict records spotted in rolled back block
        deposit_conflicts::delete_conflicted_at(pgtx, header.height).await;

        // Apply supply patch if one was prepared
        if let Some(ref patch) = self.rollback_patch {
            supply::patch_deposits(pgtx, patch).await;
        }
    }
}

impl PatchableStore for SpecStore {
    type P = Vec<SupplyDiff>;

    fn stage_rollback_patch(&mut self, patch: Self::P) {
        if patch.is_empty() {
            self.rollback_patch = None;
        } else {
            self.rollback_patch = Some(patch);
        }
    }
}

pub(super) async fn load_parser_cache(client: &Client) -> ParserCache {
    let supply = supply::get_latest(client).await.unwrap_or(SupplyRecord {
        height: -1,
        main: 0,
        deposits: 0,
    });
    let main_addresses = main_addresses::map_all(client).await;
    let deposit_addresses = deposit_addresses::map_all(client).await;
    let deposit_conflicts = deposit_conflicts::map_all(client).await;
    let deposit_ignored = deposit_ignored::get_all(client).await;

    ParserCache {
        supply,
        main_addresses,
        deposit_addresses,
        deposit_conflicts,
        deposit_ignored,
    }
}

impl Store {
    pub(super) async fn get_deposit_addresses_spotted_at(&self, height: Height) -> Vec<AddressID> {
        deposit_addresses::get_spotted_at(self.get_client(), height).await
    }

    pub(super) async fn get_deposit_conflicts_spotted_at(
        &self,
        height: Height,
    ) -> Vec<DepositAddressConflictRecord> {
        deposit_conflicts::get_conflicted_at(self.get_client(), height).await
    }
}

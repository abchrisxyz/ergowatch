use async_trait::async_trait;
use std::collections::HashMap;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::AddressID;
use crate::core::types::Header;
use crate::framework::store::BatchStore;
use crate::framework::store::PgStore;
use crate::framework::store::Revision;
use crate::framework::store::StoreDef;
use crate::framework::StampedData;

use super::parsing::ParserCache;
use super::types::BalanceRecord;
use super::Batch;
use super::WORKER_ID;
use crate::constants::settings::ROLLBACK_HORIZON;

mod balances;
mod composition;
mod counts;

pub const SCHEMA: StoreDef = StoreDef {
    schema_name: "erg",
    worker_id: WORKER_ID,
    sql: include_str!("store/schema.sql"),
    revision: &Revision { major: 1, minor: 1 },
};

pub(super) type Store = PgStore<SpecStore>;
pub(super) struct SpecStore;

#[async_trait]
impl BatchStore for SpecStore {
    type B = Batch;

    async fn new() -> Self {
        Self {}
    }

    async fn persist(&mut self, pgtx: &Transaction<'_>, stamped_batch: &StampedData<Self::B>) {
        let batch = &stamped_batch.data;
        // timestamps::insert(&pgtx, stamped_batch.height, stamped_batch.timestamp).await;

        /*
           h = height of current batch
           log address id of new balances at h (to be removed on rollback)
           log spent and modified balances at h-1 (to be inserted on rollback)
           delete logs odler than h - ROLLBACk_HORIZON
        */
        // Before modifying any balances, log current state to allow rollbacks.
        let height = stamped_batch.height;
        let modified_addresses = batch
            .balance_records
            .iter()
            // .filter(|r| r.mean_age_timestamp != stamped_batch.timestamp)
            .map(|r| r.address_id)
            .collect();
        // Log addresses that will get created
        balances::logs::log_new_balances(pgtx, height, &batch.new_addresses).await;
        // Log current balance of addresses that will get modified
        balances::logs::log_existing_balances(pgtx, height, &modified_addresses).await;
        // Log current balance of addresses that will get spent
        balances::logs::log_existing_balances(pgtx, height, &batch.spent_addresses).await;
        // Delete old logs
        balances::logs::delete_logs_prior_to(pgtx, height - ROLLBACK_HORIZON).await;

        balances::upsert_many(&pgtx, &batch.balance_records).await;
        balances::delete_many(&pgtx, &batch.spent_addresses).await;

        counts::insert(&pgtx, &batch.address_counts).await;
        composition::insert(&pgtx, &batch.supply_composition).await;
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        tracing::debug!("rolling back block {}", header.height);

        let height = header.height;
        balances::upsert_many(&pgtx, &balances::logs::get_balances_at(pgtx, height).await).await;
        balances::delete_many(
            &pgtx,
            &balances::logs::get_addresses_created_at(pgtx, height).await,
        )
        .await;
        balances::logs::delete_logs_at(pgtx, height).await;

        counts::delete_at(&pgtx, header.height).await;
        composition::delete_at(&pgtx, header.height).await;
    }
}

pub(super) async fn load_parser_cache(client: &Client) -> ParserCache {
    ParserCache {
        last_address_counts: counts::get_last(&client).await,
        last_supply_composition: composition::get_last(&client).await,
    }
}

impl Store {
    /// Retrieve and map balance records for given address id's.
    ///
    /// Does not inlcude zero balances.
    pub(super) async fn map_balance_records(
        &self,
        address_ids: Vec<AddressID>,
    ) -> HashMap<AddressID, BalanceRecord> {
        // TODO: cache
        let recs = balances::get_many(self.get_client(), &address_ids).await;
        let mut map = HashMap::new();
        for r in recs {
            map.insert(r.address_id, r);
        }
        map
    }
}

pub(super) mod migrations {
    use async_trait::async_trait;
    use tokio_postgres::Transaction;

    use crate::core::types::Height;
    use crate::framework::store::Migration;
    use crate::framework::store::MigrationEffect;
    use crate::framework::store::Revision;

    /// Migration for revision 1.1
    #[derive(Debug)]
    pub struct Mig1_1 {}

    #[async_trait]
    impl Migration for Mig1_1 {
        fn description(&self) -> &'static str {
            "Fix rollback issue"
        }

        fn revision(&self) -> Revision {
            Revision::new(1, 1)
        }

        async fn run(&self, pgtx: &Transaction<'_>) -> MigrationEffect {
            // Check if erg worker is lagging behind others.
            // If it is, then it is likely we ran into the rollback issue
            // of v1.1.0 and the erg worker state is now corrupt.
            // To fix it, reset all erg worker state so that the workers starts
            // from scratch.

            // Read current height of erg worker
            // let h = Store::get_height(pgtx).await;
            let current_height: Height = match pgtx
                .query_opt(
                    "select height from ew.headers where worker_id = 'erg';",
                    &[],
                )
                .await
                .unwrap()
            {
                Some(row) => row.get(0),
                None => {
                    return MigrationEffect::None;
                }
            };

            // Read height of upstream worker (erg_diffs)
            let upstream_height: Height = pgtx
                .query_one(
                    "select height from ew.headers where worker_id = 'erg_diffs';",
                    &[],
                )
                .await
                .map(|row| row.get(0))
                .unwrap();

            if upstream_height - current_height > 10 {
                // Too far behind upstream worker.
                // Consider we got hit by the rollback issue and reset the whole erg store.
                let tables = vec![
                    "erg.balances",
                    "erg._log_balances_previous_state_at",
                    "erg._log_balances_created_at",
                    "erg.address_counts_by_balance_p2pk",
                    "erg.address_counts_by_balance_contracts",
                    "erg.address_counts_by_balance_miners",
                    "erg.address_counts_by_balance_p2pk_summary",
                    "erg.address_counts_by_balance_contracts_summary",
                    "erg.address_counts_by_balance_miners_summary",
                    "erg.supply_composition",
                ];
                for table in tables {
                    tracing::debug!("truncating table {table}");
                    let stmt = format!("truncate table {table};");
                    pgtx.execute(&stmt, &[]).await.unwrap();
                }
                MigrationEffect::Reset
            } else {
                MigrationEffect::None
            }
        }
    }
}

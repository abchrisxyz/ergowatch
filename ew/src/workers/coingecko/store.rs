mod ergusd_block;
mod ergusd_hourly;
mod ergusd_provisional;

use async_trait::async_trait;
use tokio_postgres::Transaction;

use crate::core::types::Header;
use crate::framework::store::BatchStore;
use crate::framework::store::PgStore;
use crate::framework::store::Revision;
use crate::framework::store::StoreDef;
use crate::framework::StampedData;

use super::types::Batch;
use super::types::BlockRecord;
use super::types::HourlyRecord;
use super::Cache;

pub(super) const SCHEMA: StoreDef = StoreDef {
    schema_name: super::WORKER_ID,
    worker_id: super::WORKER_ID,
    sql: include_str!("store/schema.sql"),
    revision: &Revision { major: 1, minor: 0 },
};

pub(super) struct InnerStore {}

pub(super) type Store = PgStore<InnerStore>;

#[async_trait]
impl BatchStore for InnerStore {
    type B = Batch;

    async fn new() -> Self {
        Self {}
    }

    async fn persist(&mut self, pgtx: &Transaction<'_>, stamped_batch: &StampedData<Self::B>) {
        ergusd_block::insert(pgtx, &stamped_batch.data.block_record).await;
        if let Some(ref pr) = stamped_batch.data.provisional_block_record {
            ergusd_provisional::insert(pgtx, pr).await;
        }
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        let height = header.height;
        tracing::debug!("rolling back block {}", height);
        ergusd_block::delete_at(pgtx, header.height).await;
        ergusd_provisional::delete_at(pgtx, height).await;
    }
}

impl Store {
    /// Add hourly records and update provisional block records
    pub(super) async fn persist_tracker_data(
        &mut self,
        hourly_records: &Vec<HourlyRecord>,
        block_updates: &Vec<BlockRecord>,
    ) {
        let client = self.get_mut_client();
        let pgtx = client.transaction().await.unwrap();

        // First sync will yield a large timeseries, so process in chunks.
        for record_chunk in hourly_records.chunks(5000) {
            ergusd_hourly::insert_many(&pgtx, record_chunk).await;
        }

        // Update block records
        ergusd_block::update_many(&pgtx, block_updates).await;

        // And remove updated blocks from provisional table
        ergusd_provisional::delete_many_at(
            &pgtx,
            &block_updates.iter().map(|br| br.height).collect(),
        )
        .await;

        pgtx.commit().await.unwrap();
    }

    /// Inserts initial hourly record with genesis timestamp.
    ///
    /// Coingecko data starts a few minutes after Ergo's genesis block.
    /// This ensures all block timestamps are covered by hourly data.
    pub(super) async fn seed_hourly_data(&self) {
        let client = self.get_client();
        match ergusd_hourly::get_latest(client).await {
            Some(_) => (),
            None => ergusd_hourly::insert(client, &HourlyRecord::genesis()).await,
        }
    }

    pub(super) async fn load_cache(&self) -> Cache {
        let client = self.get_client();

        let provisional_records = ergusd_provisional::get_all(client).await;

        let recent_hourly_records = match provisional_records.first() {
            Some(first_provisional_record) => {
                let since =
                    ergusd_hourly::get_last_prior_to(client, first_provisional_record.timestamp)
                        .await
                        .unwrap_or(HourlyRecord::genesis());
                ergusd_hourly::get_since(client, since.timestamp).await
            }
            None => {
                let last_hourly_record = ergusd_hourly::get_latest(client)
                    .await
                    .unwrap_or(HourlyRecord::genesis());
                vec![last_hourly_record]
            }
        };

        Cache {
            recent_hourly_records,
            provisional_records,
        }
    }
}

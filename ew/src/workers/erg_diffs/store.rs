mod diffs;

use async_trait::async_trait;

use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::store::BatchStore;
use crate::framework::store::PgStore;
use crate::framework::store::Revision;
use crate::framework::store::SourcableStore;
use crate::framework::store::StoreDef;
use crate::framework::StampedData;

use super::types::Batch;
use super::types::DiffData;
use super::WORKER_ID;

pub(super) const SCHEMA: StoreDef = StoreDef {
    schema_name: "erg",
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
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        tracing::debug!("rolling back block {}", header.height);
        diffs::delete_at(&pgtx, header.height).await;
    }
}

#[async_trait]
impl SourcableStore for InnerStore {
    type S = DiffData;

    async fn get_at(&self, client: &Client, height: Height) -> Self::S {
        DiffData {
            diff_records: diffs::select_at(client, height).await,
        }
    }
}

mod diffs;

use async_trait::async_trait;

use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::types::DiffRecord;
use crate::core::types::Header;
use crate::framework::store::BatchStore;
use crate::framework::store::PgStore;
use crate::framework::store::Revision;
use crate::framework::store::SourcableStore;
use crate::framework::store::StoreDef;
use crate::framework::utils::BlockRange;
use crate::framework::StampedData;

use super::queries;
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

    async fn get_slice(&self, client: &Client, block_range: &BlockRange) -> Vec<Self::S> {
        let diff_records =
            diffs::select_slice(client, block_range.first_height, block_range.last_height).await;
        partition_diff_records(diff_records, block_range.size() as usize)
    }
}

impl Store {
    pub(super) async fn query_balance_diffs(
        &self,
        query: queries::DiffsQuery,
    ) -> queries::DiffsQueryResponse {
        diffs::select_aggregate_series(&self.get_client(), &query.address_ids).await
    }
}

/// Partition diff records by height into `DiffData`'s
///
/// * `diff_records`: collection of diff records ordered by height
/// * `n`: number of expected heights
fn partition_diff_records(diff_records: Vec<DiffRecord>, n: usize) -> Vec<DiffData> {
    let mut slice: Vec<DiffData> = Vec::with_capacity(n);
    let mut i = 0;
    let mut h = diff_records[0].height;

    // Init first item
    slice.push(DiffData {
        diff_records: vec![],
    });

    for rec in diff_records.into_iter() {
        if rec.height == h {
            // Same h - append to existing item;
            slice[i].diff_records.push(rec);
        } else if rec.height == h + 1 {
            // Next h, start new item
            i += 1;
            h += 1;
            slice.push(DiffData {
                diff_records: vec![rec],
            });
        } else {
            panic!("unordered diff records")
        }
    }
    slice
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::types::AddressID;
    use crate::core::types::Height;

    #[test]
    fn test_partition_diff_records() {
        let diff_records = vec![
            DiffRecord {
                height: 10,
                address_id: AddressID(123),
                tx_idx: 0,
                nano: 1_000_000_000,
            },
            DiffRecord {
                height: 10,
                address_id: AddressID(123),
                tx_idx: 0,
                nano: 1_000_000_000,
            },
            DiffRecord {
                height: 11,
                address_id: AddressID(123),
                tx_idx: 0,
                nano: 1_000_000_000,
            },
            DiffRecord {
                height: 11,
                address_id: AddressID(123),
                tx_idx: 0,
                nano: 1_000_000_000,
            },
            DiffRecord {
                height: 12,
                address_id: AddressID(123),
                tx_idx: 0,
                nano: 1_000_000_000,
            },
        ];

        let slice = partition_diff_records(diff_records, 1);

        assert_eq!(slice.len(), 3);
        assert_eq!(
            slice[0]
                .diff_records
                .iter()
                .map(|r| r.height)
                .collect::<Vec<Height>>(),
            vec![10, 10]
        );
        assert_eq!(
            slice[1]
                .diff_records
                .iter()
                .map(|r| r.height)
                .collect::<Vec<Height>>(),
            vec![11, 11]
        );
        assert_eq!(
            slice[2]
                .diff_records
                .iter()
                .map(|r| r.height)
                .collect::<Vec<Height>>(),
            vec![12]
        );
    }
}

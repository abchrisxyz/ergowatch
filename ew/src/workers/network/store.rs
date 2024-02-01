mod mining;
mod parameters;
mod proposals;
mod transactions;
mod unhandled_extensions;
mod votes;

use async_trait::async_trait;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

use super::parsing::ParserCache;
use super::types::Difficulty;
use super::types::Proposal;
use super::Batch;
use crate::core::types::Header;
use crate::framework::store::BatchStore;
use crate::framework::store::PgStore;
use crate::framework::store::Revision;
use crate::framework::store::StoreDef;
use crate::framework::StampedData;

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
        let batch = match &stamped_batch.data {
            Batch::Genesis => return,
            Batch::Block(data) => data,
        };

        // Network parameters
        if let Some(record) = &batch.parameters {
            parameters::insert(pgtx, &record).await;
        }

        // Votes
        votes::insert(pgtx, &batch.votes).await;

        // Proposal
        match &batch.proposal {
            Proposal::New(record) => proposals::insert(pgtx, record).await,
            Proposal::Tally(record) => proposals::update(pgtx, record).await,
            Proposal::Empty => (),
        }

        // Mining
        mining::insert(pgtx, &batch.mining).await;

        // Unhandled extensions
        unhandled_extensions::insert_many(pgtx, &batch.unhandled_extensions).await;

        // Transaction counts
        transactions::insert(pgtx, &batch.transactions).await;
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        let height = header.height;
        tracing::debug!("rolling back block {}", height);

        // Read last proposal
        let proposal = proposals::get_last(pgtx).await.unwrap();
        if proposal.height == height {
            // Delete proposal created in rolled back block
            proposals::delete_at(pgtx, height).await;
        } else {
            // Proposal created in earlier block, reverse tally
            let votes = votes::get_at(pgtx, height).await.unwrap();
            let previous = proposal.withdraw_votes(votes.pack());
            proposals::update(pgtx, &previous).await;
        }

        // Delete records at height
        parameters::delete_at(pgtx, height).await;
        votes::delete_at(pgtx, height).await;
        mining::delete_at(pgtx, height).await;
        unhandled_extensions::delete_at(pgtx, height).await;
        transactions::delete_at(pgtx, height).await;
    }
}

pub(super) async fn load_parser_cache(client: &Client) -> ParserCache {
    let difficulties = load_diff_cache(client).await;
    ParserCache::new(proposals::get_last(client).await, difficulties)
}

/// Loads timestamp and difficulty from last recorded 24h window.
async fn load_diff_cache(client: &Client) -> Vec<(i64, Difficulty)> {
    let sql = "
        select h.timestamp,
            m.difficulty
        from network.mining m
        join core.headers h on h.height = m.height
        where h.timestamp >= (
            select h.timestamp - 86400000
            from network.mining m
            join core.headers h on h.height = m.height
            order by h.height desc limit 1
        )
        order by m.height;
    ";
    client
        .query(sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

use async_trait::async_trait;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::Header;
use crate::framework::store::BatchStore;
use crate::framework::store::PgStore;
use crate::framework::store::Schema;
use crate::framework::StampedData;

use super::parsing::ParserCache;
use super::types::Action;
use super::types::Batch;
use super::types::TimestampRecord;

mod daily;
mod hourly;
mod timestamps;
mod weekly;

pub(super) const SCHEMA: Schema = Schema {
    name: "timestamps",
    sql: include_str!("store/schema.sql"),
};

pub(super) struct SpecStore {}

pub(super) type Store = PgStore<SpecStore>;

#[async_trait]
impl BatchStore for SpecStore {
    type B = Batch;

    async fn new() -> Self {
        Self {}
    }

    async fn persist(&mut self, pgtx: &Transaction<'_>, stamped_batch: &StampedData<Self::B>) {
        // Timestamps
        timestamps::insert(&pgtx, stamped_batch.height, stamped_batch.timestamp).await;

        // Hourly
        for action in &stamped_batch.data.hourly {
            // tracing::debug!("hourly...");
            match action {
                Action::INSERT(record) => hourly::insert(&pgtx, record).await,
                Action::UPDATE(record) => hourly::update(&pgtx, record).await,
                Action::DELETE(height) => hourly::delete_at(&pgtx, height).await,
            }
        }

        // Daily
        for action in &stamped_batch.data.daily {
            match action {
                Action::INSERT(record) => daily::insert(&pgtx, record).await,
                Action::UPDATE(record) => daily::update(&pgtx, record).await,
                Action::DELETE(height) => daily::delete_at(&pgtx, height).await,
            }
        }

        // Weekly
        for action in &stamped_batch.data.weekly {
            match action {
                Action::INSERT(record) => weekly::insert(&pgtx, record).await,
                Action::UPDATE(record) => weekly::update(&pgtx, record).await,
                Action::DELETE(height) => weekly::delete_at(&pgtx, height).await,
            }
        }
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        // Delete timestamps after parent's block's timestamp and reinsert
        // parent block's timestamp if necessary.
        // In examples below, we roll back block 8.
        //
        // blocks :     6     7        8
        // windows:        |        |
        // before :        6        7  8
        // after  :        6  7
        //
        // blocks :     5      6       7   8
        // windows:        |        |
        // before :        5        6      8
        // after  :        5        6  7
        //
        // blocks :     5     6  7  8
        // windows:        |        |
        // before :        5        8
        // after  :        5     7

        let height = header.height;
        tracing::debug!("rolling back block {}", height);

        timestamps::delete_at(&pgtx, height).await;
        let parent_height = height - 1;
        let parent_timestamp = timestamps::get_at(pgtx, parent_height).await;

        // Delete timestamps past new latest one
        hourly::delete_after(&pgtx, parent_timestamp).await;
        daily::delete_after(&pgtx, parent_timestamp).await;
        weekly::delete_after(&pgtx, parent_timestamp).await;

        // Reinsert last hourly timestamp if needed
        let last = hourly::get_last(pgtx)
            .await
            .expect("always data left after a roll back");
        if last.height < parent_height {
            hourly::insert(
                &pgtx,
                &TimestampRecord::new(parent_height, parent_timestamp),
            )
            .await;
        }

        // Reinsert last daily timestamp if needed
        let last = daily::get_last(pgtx)
            .await
            .expect("always data left after a roll back");
        if last.height < parent_height {
            daily::insert(
                &pgtx,
                &TimestampRecord::new(parent_height, parent_timestamp),
            )
            .await;
        }

        // Reinsert last weekly timestamp if needed
        let last = weekly::get_last(pgtx)
            .await
            .expect("always data left after a roll back");
        if last.height < parent_height {
            weekly::insert(
                &pgtx,
                &TimestampRecord::new(parent_height, parent_timestamp),
            )
            .await;
        }
    }
}

pub(super) async fn load_parser_cache(client: &Client) -> ParserCache {
    ParserCache {
        last_hourly: hourly::get_last(client)
            .await
            .unwrap_or(TimestampRecord::initial()),
        last_daily: daily::get_last(client)
            .await
            .unwrap_or(TimestampRecord::initial()),
        last_weekly: weekly::get_last(client)
            .await
            .unwrap_or(TimestampRecord::initial()),
    }
}

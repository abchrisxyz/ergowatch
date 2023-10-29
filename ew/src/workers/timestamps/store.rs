use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::utils::Schema;

use super::parsing::ParserCache;
use super::types::Action;
use super::types::Batch;
use super::types::TimestampRecord;

mod daily;
mod headers;
mod hourly;
mod weekly;

pub(super) struct Store {
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

        let schema = Schema::new("timestamps", include_str!("store/schema.sql"));
        schema.init(&mut client).await;

        let head = headers::get_last(&client)
            .await
            .map_or(Head::initial(), |h| h.head());
        tracing::debug!("head: {:?}", &head);

        Self { client, head }
    }

    pub(super) fn get_head(&self) -> &Head {
        &self.head
    }

    pub(super) async fn load_parser_cache(&self) -> ParserCache {
        ParserCache {
            last_hourly: hourly::get_last(&self.client)
                .await
                .unwrap_or(TimestampRecord::initial()),
            last_daily: daily::get_last(&self.client)
                .await
                .unwrap_or(TimestampRecord::initial()),
            last_weekly: weekly::get_last(&self.client)
                .await
                .unwrap_or(TimestampRecord::initial()),
        }
    }

    pub(super) async fn persist(&mut self, batch: Batch) {
        tracing::debug!("persisting data for block {}", batch.header.height);
        let pgtx = self.client.transaction().await.unwrap();

        // Header
        headers::insert(&pgtx, &batch.header).await;

        // Hourly
        for action in &batch.hourly {
            // tracing::debug!("hourly...");
            match action {
                Action::INSERT(record) => hourly::insert(&pgtx, record).await,
                Action::UPDATE(record) => hourly::update(&pgtx, record).await,
                Action::DELETE(height) => hourly::delete_at(&pgtx, height).await,
            }
        }

        // Daily
        for action in &batch.daily {
            match action {
                Action::INSERT(record) => daily::insert(&pgtx, record).await,
                Action::UPDATE(record) => daily::update(&pgtx, record).await,
                Action::DELETE(height) => daily::delete_at(&pgtx, height).await,
            }
        }

        // Weekly
        for action in &batch.weekly {
            match action {
                Action::INSERT(record) => weekly::insert(&pgtx, record).await,
                Action::UPDATE(record) => weekly::update(&pgtx, record).await,
                Action::DELETE(height) => weekly::delete_at(&pgtx, height).await,
            }
        }

        pgtx.commit().await.unwrap();

        // Update head
        self.head = batch.header.head();
    }

    /// Roll back changes from last block.
    pub(super) async fn roll_back(&mut self, height: Height) {
        tracing::debug!("rolling back block {}", height);
        assert_eq!(self.head.height, height);

        let pgtx = self.client.transaction().await.unwrap();

        // Delete header at h
        headers::delete_at(&pgtx, height).await;
        let last_timestamp = headers::get_last(&pgtx).await.unwrap().timestamp;

        // Delete timestamps past new latest one
        hourly::delete_after(&pgtx, last_timestamp).await;
        daily::delete_after(&pgtx, last_timestamp).await;
        weekly::delete_after(&pgtx, last_timestamp).await;

        pgtx.commit().await.unwrap();

        // Reload head
        let header = headers::get_last(&self.client).await.unwrap();
        self.head = Head {
            height: header.height,
            header_id: header.id,
        }
    }
}

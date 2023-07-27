use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::utils::Schema;

use super::parsing::ParserCache;
use super::types::Event;
use super::Batch;

mod bank_transactions;
mod headers;
mod history;
mod ohlcs;
mod oracle_postings;
mod services;

pub struct Store {
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

        let schema = Schema::new("core", include_str!("store/schema.sql"));
        schema.init(&mut client).await;

        let head = headers::get(&client).await.head();
        tracing::debug!("head: {:?}", &head);

        Self { client, head }
    }

    pub(super) fn get_head(&self) -> &Head {
        &self.head
    }

    pub(super) async fn load_parser_cache(&self) -> ParserCache {
        ParserCache {
            bank_transaction_count: bank_transactions::get_count(&self.client).await,
            last_history_record: history::get_latest(&self.client).await,
            last_ohlc_group: ohlcs::get_latest_group(&self.client).await,
        }
    }

    pub(super) async fn persist(&mut self, batch: Batch) {
        tracing::debug!("persisting data for block {}", batch.header.height);
        let pgtx = self.client.transaction().await.unwrap();

        // Header
        headers::insert(&pgtx, &batch.header).await;

        // Events
        for event in &batch.events {
            match event {
                Event::Oracle(op) => oracle_postings::insert(&pgtx, op).await,
                Event::BankTx(btx) => bank_transactions::insert(&pgtx, btx).await,
            }
        }

        // History record
        if let Some(hr) = batch.history_record {
            history::insert(&pgtx, &hr).await;
        }

        // OHLC's
        let height = batch.header.height;
        for record in &batch.daily_ohlc_records {
            ohlcs::upsert_daily(&pgtx, &record, height).await;
        }
        for record in &batch.weekly_ohlc_records {
            ohlcs::upsert_weekly(&pgtx, &record, height).await;
        }
        for record in &batch.monthly_ohlc_records {
            ohlcs::upsert_monthly(&pgtx, &record, height).await;
        }

        // Service diffs
        for diff in &batch.service_diffs {
            services::upsert(&pgtx, diff).await;
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

        // Delete bank txs at h
        bank_transactions::detele_at(&pgtx, height).await;

        // Delete oracle postings at h
        oracle_postings::delete_at(&pgtx, height).await;

        // Delete history at h
        history::delete_at(&pgtx, height).await;

        // Recreate service stats from scratch
        services::refresh(&pgtx).await;

        // Restore ohlc from log
        ohlcs::roll_back_daily(&pgtx, height).await;
        ohlcs::roll_back_weekly(&pgtx, height).await;
        ohlcs::roll_back_monthly(&pgtx, height).await;

        pgtx.commit().await.unwrap();

        // Reload head
        let header = headers::get(&self.client).await;
        self.head = Head {
            height: header.height,
            header_id: header.id,
        }
    }
}

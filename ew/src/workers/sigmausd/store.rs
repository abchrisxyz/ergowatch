use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::utils::Schema;

use super::parsing::ParserCache;
use super::types::Event;
use super::Batch;

mod bank_transactions;
mod head;
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

        let head = head::get(&client).await;
        tracing::debug!("head: {:?}", &head);

        Self { client, head }
    }

    pub(super) async fn get_head(&self) -> Head {
        head::get(&self.client).await
    }

    pub(super) async fn load_parser_cache(&self) -> ParserCache {
        ParserCache {
            bank_transaction_count: bank_transactions::get_count(&self.client).await,
            last_oracle_posting: oracle_postings::get_latest(&self.client).await,
            last_history_record: history::get_latest(&self.client).await,
            last_ohlc_group: ohlcs::get_latest_group(&self.client).await,
        }
    }

    pub(super) async fn persist(&mut self, batch: Batch) {
        tracing::debug!("persisting data for block {}", batch.head.height);
        let pgtx = self.client.transaction().await.unwrap();

        // Head
        head::update(&pgtx, &batch.head).await;

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
        for record in &batch.daily_ohlc_records {
            ohlcs::update_daily(&pgtx, &record).await;
        }
        for record in &batch.weekly_ohlc_records {
            ohlcs::update_weekly(&pgtx, &record).await;
        }
        for record in &batch.monthly_ohlc_records {
            ohlcs::update_monthly(&pgtx, &record).await;
        }

        // Service diffs
        for diff in &batch.service_diffs {
            services::upsert(&pgtx, diff).await;
        }

        pgtx.commit().await.unwrap();
    }

    pub(super) async fn roll_back(&mut self, height: Height) {
        tracing::debug!("rolling back block {}", height);
        todo!()
    }
}

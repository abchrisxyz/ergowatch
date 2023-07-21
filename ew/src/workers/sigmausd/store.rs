use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::utils::Schema;

use super::parsing::ParserCache;
use super::Batch;

mod bank_transactions;
mod head;
mod history;
mod ohlcs;
mod oracle_postings;

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

        tracing::warn!("Using dummy head");
        let head = Head::initial(); //blocks::last_head(&client).await;
        tracing::debug!("head: {:?}", &head);

        Self { client, head }
    }

    pub(super) fn get_head(&self) -> &Head {
        &self.head
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
            todo!()
        }

        // History record
        if let Some(hr) = batch.history_record {
            todo!()
        }

        // OHLC's

        // Service diffs
        for diff in &batch.service_diffs {
            todo!()
        }

        pgtx.commit().await.unwrap();
    }

    pub(super) async fn roll_back(&mut self, height: Height) {
        tracing::debug!("rolling back block {}", height);
        todo!()
    }
}

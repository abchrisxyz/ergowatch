use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::utils::Schema;

use super::Batch;

mod head;
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

    async fn persist(&mut self, batch: Batch) {
        tracing::debug!("persisting data for block {}", batch.head.height);
        let pgtx = self.client.transaction().await.unwrap();

        // Head
        head::update(&pgtx, &batch.head).await;

        // Bank transactions
        for bank_tx in &batch.bank_transactions {
            todo!()
        }

        // Oracle posting
        if let Some(op) = batch.oracle_posting {
            oracle_postings::insert(&pgtx, &op).await;
        }

        // History record
        if let Some(hr) = batch.history_record {
            todo!()
        }

        // Service diffs
        for diff in &batch.service_diffs {
            todo!()
        }

        // OHLC's

        pgtx.commit().await.unwrap();
    }
    async fn roll_back(&mut self, height: Height) {
        tracing::debug!("rolling back block {}", height);
        todo!()
    }
}

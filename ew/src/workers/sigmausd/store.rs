use async_trait::async_trait;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::Header;
use crate::framework::store::BatchStore;
use crate::framework::store::PgStore;
use crate::framework::store::Revision;
use crate::framework::store::StoreDef;
use crate::framework::StampedData;

use super::parsing::ParserCache;
use super::types::Event;
use super::Batch;

mod bank_transactions;
mod history;
mod noop_bank_transactions;
mod ohlcs;
mod oracle_postings;
mod services;

pub(super) const SCHEMA: StoreDef = StoreDef {
    schema_name: super::WORKER_ID,
    worker_id: super::WORKER_ID,
    sql: include_str!("store/schema.sql"),
    revision: &Revision { major: 1, minor: 2 },
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
        let batch = &stamped_batch.data;
        // Events
        for event in &batch.events {
            match event {
                Event::Oracle(op) => oracle_postings::insert(&pgtx, op).await,
                Event::BankTx(btx) => bank_transactions::insert(&pgtx, btx).await,
                Event::NoopBankTx(ntx) => noop_bank_transactions::insert(&pgtx, ntx).await,
            }
        }

        // History record
        if let Some(ref hr) = batch.history_record {
            history::insert(&pgtx, &hr).await;
        }

        // OHLC's
        let height = stamped_batch.height;
        ohlcs::upsert_daily_records(&pgtx, &batch.daily_ohlc_records, height).await;
        ohlcs::upsert_weekly_records(&pgtx, &batch.weekly_ohlc_records, height).await;
        ohlcs::upsert_monthly_records(&pgtx, &batch.monthly_ohlc_records, height).await;

        // Service diffs
        for diff in &batch.service_diffs {
            services::upsert(&pgtx, diff).await;
        }
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        let height = header.height;
        tracing::debug!("rolling back block {}", height);
        // assert_eq!(self.head.height, height);

        // Delete bank txs at h
        bank_transactions::detele_at(&pgtx, height).await;
        noop_bank_transactions::detele_at(&pgtx, height).await;

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
    }
}

pub(super) async fn load_parser_cache(client: &Client) -> ParserCache {
    ParserCache {
        bank_transaction_count: bank_transactions::get_count(client).await,
        last_history_record: history::get_latest(client).await,
        last_ohlc_group: ohlcs::get_latest_group(client).await,
    }
}

pub(super) mod migrations {
    use async_trait::async_trait;
    use tokio_postgres::Transaction;

    use crate::framework::store::Migration;
    use crate::framework::store::MigrationEffect;
    use crate::framework::store::Revision;

    /// Migration for revision 1.1
    #[derive(Debug)]
    pub struct Mig1_1 {}

    #[async_trait]
    impl Migration for Mig1_1 {
        fn description(&self) -> &'static str {
            "Fix sigmausd oracle prep box update"
        }

        fn revision(&self) -> Revision {
            Revision::new(1, 1)
        }

        async fn run(&self, pgtx: &Transaction<'_>) -> MigrationEffect {
            // Remove sigmausd schema
            pgtx.execute("drop schema if exists sigmausd cascade;", &[])
                .await
                .unwrap();

            MigrationEffect::Purge
        }
    }

    /// Migration for revision 1.1
    #[derive(Debug)]
    pub struct Mig1_2 {}

    #[async_trait]
    impl Migration for Mig1_2 {
        fn description(&self) -> &'static str {
            "Handle no-op bank transactions"
        }

        fn revision(&self) -> Revision {
            Revision::new(1, 2)
        }

        async fn run(&self, pgtx: &Transaction<'_>) -> MigrationEffect {
            // Remove sigmausd schema
            pgtx.execute(
                "
                create table sigmausd.noop_bank_transactions (
                    height integer not null,
                    tx_idx integer not null,
                    tx_id text primary key,
                    box_id text not null
                );",
                &[],
            )
            .await
            .unwrap();

            MigrationEffect::None
        }
    }
}

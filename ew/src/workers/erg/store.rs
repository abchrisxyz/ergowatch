use itertools::Itertools;
use std::collections::HashMap;
use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::core::types::AddressID;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::utils::Schema;

use super::parsing::Balance;
use super::parsing::ParserCache;
use super::types::BalanceRecord;
use super::Batch;

mod balances;
mod composition;
mod counts;
mod diffs;
mod headers;

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

        let schema = Schema::new("erg", include_str!("store/schema.sql"));
        schema.init(&mut client).await;

        let head = headers::get_last(&client).await.head();
        tracing::debug!("head: {:?}", &head);

        Self { client, head }
    }

    pub(super) fn get_head(&self) -> &Head {
        &self.head
    }

    pub(super) async fn load_parser_cache(&self) -> ParserCache {
        ParserCache {
            last_address_counts: counts::get_last(&self.client).await,
            last_supply_composition: composition::get_last(&self.client).await,
        }
    }

    pub(super) async fn persist(&mut self, batch: Batch) {
        // tracing::debug!("persisting data for block {}", batch.header.height);
        let pgtx = self.client.transaction().await.unwrap();

        headers::insert(&pgtx, &batch.header).await;
        diffs::insert_many(&pgtx, &batch.diff_records).await;
        balances::upert_many(&pgtx, &batch.balance_records).await;
        balances::delete_many(&pgtx, &batch.spent_addresses).await;
        counts::insert(&pgtx, &batch.address_counts).await;
        composition::insert(&pgtx, &batch.supply_composition).await;

        pgtx.commit().await.unwrap();

        // Update head
        self.head = batch.header.head();
    }

    /// Roll back changes from last block.
    pub(super) async fn roll_back(&mut self, height: Height) {
        tracing::debug!("rolling back block {}", height);
        assert_eq!(self.head.height, height);

        let pgtx = self.client.transaction().await.unwrap();

        // Retrieve current diffs and balances to determine how to restore balances.
        // Retrieve diff records at current height.
        let diff_records = diffs::select_at(&pgtx, height).await;
        // Retrieve current balances for diffed addresses
        let address_ids: Vec<AddressID> =
            diff_records.iter().map(|r| r.address_id).unique().collect();
        let balance_records = balances::get_many(&pgtx, &address_ids).await;

        let spent_address_ids = vec![];
        for address_id in spent_address_ids {
            let timestamped_diffs = diffs::get_address_diffs(&pgtx, address_id).await;
            let balance: Option<Balance> = timestamped_diffs
                .into_iter()
                .fold(None, |acc, (nano, timestamp)| {
                    Some(Balance::new(nano, timestamp))
                });
        }

        // Restore balances

        let balances_to_restore = diffs::delete_at(&pgtx, height).await;

        headers::delete_at(&pgtx, height).await;
        counts::delete_at(&pgtx, height).await;
        composition::delete_at(&pgtx, height).await;

        pgtx.commit().await.unwrap();

        // Reload head
        let header = headers::get_last(&self.client).await;
        self.head = Head {
            height: header.height,
            header_id: header.id,
        }
    }

    /// Retrieve and map balance records for given address id's.
    ///
    /// Does not inlcude zero balances.
    pub(super) async fn map_balance_records(
        &self,
        address_ids: Vec<AddressID>,
    ) -> HashMap<AddressID, BalanceRecord> {
        // TODO: cache
        let recs = balances::get_many(&self.client, &address_ids).await;
        let mut map = HashMap::new();
        for r in recs {
            map.insert(r.address_id, r);
        }
        map
    }
}

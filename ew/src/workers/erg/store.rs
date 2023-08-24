use std::collections::HashMap;
use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::core::types::AddressID;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::utils::Schema;

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

        todo!();

        pgtx.commit().await.unwrap();

        // Update head
        self.head = batch.header.head();
    }

    /// Roll back changes from last block.
    pub(super) async fn roll_back(&mut self, height: Height) {
        tracing::debug!("rolling back block {}", height);
        assert_eq!(self.head.height, height);

        let pgtx = self.client.transaction().await.unwrap();

        todo!();

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

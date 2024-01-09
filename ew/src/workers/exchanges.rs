mod parsing;
mod store;
mod types;

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::config::PostgresConfig;
use crate::core::types::AddressID;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::EventHandling;
use crate::framework::QuerySender;
use crate::framework::Querying;
use crate::framework::StampedData;
use crate::workers::erg_diffs::queries::DiffsQuery;
use crate::workers::erg_diffs::queries::DiffsQueryResponse;
use crate::workers::erg_diffs::types::DiffData;
use parsing::Parser;
use store::Store;

use self::types::SupplyDiff;

// Exposing for tests
pub use self::types::SupplyRecord;

const WORKER_ID: &'static str = "exchanges";

pub type Worker = crate::framework::LeafWorker<CexWorkFlow>;

pub struct CexWorkFlow {
    parser: Parser,
    store: Store,
    query_sender: QuerySender<DiffsQuery, DiffsQueryResponse>,
}

#[async_trait]
impl EventHandling for CexWorkFlow {
    type U = DiffData;
    type D = ();

    async fn new(pgconf: &PostgresConfig) -> Self {
        let store = Store::new(pgconf, &store::SCHEMA).await;
        let cache = store::load_parser_cache(store.get_client()).await;
        let parser = Parser::new(cache);
        Self {
            parser,
            store,
            query_sender: QuerySender::placeholder(),
        }
    }

    #[tracing::instrument(skip(self, data), fields(height = data.height))]
    async fn include_block(&mut self, data: &StampedData<DiffData>) -> Self::D {
        // Obtain deposit address spottings.
        // Supply on new deposit addresses must be added retroactively to total deposit supply.
        // Supply on addresses spotted as inter-block conflicts must be subtracted from
        // total deposit supply.
        let spottings = self.parser.spot_deposit_addresses(&data);

        // Query balance changes for new deposit addresses (positive supply changes)
        let pos_rx = match spottings.new_deposits.is_empty() {
            true => None,
            false => {
                let query = DiffsQuery::new(
                    spottings.new_deposits.keys().map(|k| *k).collect(),
                    data.height,
                );
                let rx = self.query_sender.send(query).await;
                Some(rx)
            }
        };

        // Query balance changes for inter-block conflicts (negative supply changes)
        let neg_rx = match spottings.inter_conflicts.is_empty() {
            true => None,
            false => {
                let query = DiffsQuery::new(
                    spottings.inter_conflicts.keys().map(|a| *a).collect(),
                    data.height,
                );
                let rx = self.query_sender.send(query).await;
                Some(rx)
            }
        };

        // Wait for queries to be processed
        let pos_diffs = match pos_rx {
            None => vec![],
            Some(rx) => {
                tracing::debug!("waiting for pos query response");
                rx.await.unwrap()
            }
        };
        let neg_diffs = match neg_rx {
            None => vec![],
            Some(rx) => {
                tracing::debug!("waiting for neg query response");
                rx.await.unwrap()
            }
        };

        // Proceed with 2nd stage of parsing
        let stamped_batch = self
            .parser
            .extract_batch(data, spottings, pos_diffs, neg_diffs);
        self.store.persist(&stamped_batch).await;
    }

    async fn roll_back(&mut self, height: Height) -> Header {
        // let self.store.get_deposit_addresses_spotted_at(height);
        // self.store.get_deposit_conflicts_spotted_at(height);

        // Query balance changes for deposit conflicts to be rolled back (positive supply changes)
        let pos_rx = self
            .query_balance_diffs(
                self.store
                    .get_deposit_conflicts_spotted_at(height)
                    .await
                    .iter()
                    .map(|r| r.address_id)
                    .collect(),
                height,
            )
            .await;

        // Query balance changes for deposit addresses to be rolled back (negative supply changes)
        let neg_rx = self
            .query_balance_diffs(
                self.store.get_deposit_addresses_spotted_at(height).await,
                height,
            )
            .await;

        // Wait for queries to be processed
        let pos_diffs = match pos_rx {
            None => vec![],
            Some(rx) => rx.await.unwrap(),
        };
        let neg_diffs = match neg_rx {
            None => vec![],
            Some(rx) => rx.await.unwrap(),
        };

        let patch = parsing::calculate_net_supply_patch(pos_diffs, neg_diffs);

        // Stage patch to be rolled back within rollback transaction
        self.store.stage_rollback_patch(patch);

        self.store.roll_back(height).await;
        // Refresh parser cache to reflect rollback
        let cache = store::load_parser_cache(self.store.get_client()).await;
        self.parser = Parser::new(cache);
        self.store.get_header().clone()
    }

    fn header<'a>(&'a self) -> &'a Header {
        self.store.get_header()
    }
}

#[async_trait]
impl Querying for CexWorkFlow {
    type Q = DiffsQuery;
    type R = DiffsQueryResponse;

    fn set_query_sender(&mut self, query_sender: QuerySender<Self::Q, Self::R>) {
        tracing::debug!("setting query sender");
        self.query_sender = query_sender;
    }
}

impl CexWorkFlow {
    async fn query_balance_diffs(
        &self,
        address_ids: Vec<AddressID>,
        max_height: Height,
    ) -> Option<oneshot::Receiver<Vec<SupplyDiff>>> {
        if address_ids.is_empty() {
            return None;
        }
        Some(
            self.query_sender
                .send(DiffsQuery::new(address_ids, max_height))
                .await,
        )
    }
}

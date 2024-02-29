mod deposit_addresses;
mod deposit_conflicts;
mod deposit_ignored;
mod exchanges;
mod main_addresses;
mod supply;

use async_trait::async_trait;
use tokio_postgres::Client;
use tokio_postgres::Transaction;

use crate::core::types::AddressID;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::framework::store::BatchStore;
use crate::framework::store::PatchableStore;
use crate::framework::store::PgStore;
use crate::framework::store::Revision;
use crate::framework::store::StoreDef;
use crate::framework::StampedData;

use super::parsing::ParserCache;
use super::types::Batch;
use super::types::DepositAddressConflict;
use super::types::DepositAddressConflictRecord;
use super::types::DepositAddressRecord;
use super::types::SupplyDiff;
use super::types::SupplyRecord;
use super::WORKER_ID;

pub const SCHEMA: StoreDef = StoreDef {
    schema_name: "exchanges",
    worker_id: WORKER_ID,
    sql: include_str!("store/schema.sql"),
    revision: &Revision { major: 1, minor: 4 },
};

pub(super) type Store = PgStore<SpecStore>;
pub(super) struct SpecStore {
    pub rollback_patch: Option<Vec<SupplyDiff>>,
}

#[async_trait]
impl BatchStore for SpecStore {
    type B = Batch;

    async fn new() -> Self {
        Self {
            rollback_patch: None,
        }
    }

    async fn persist(&mut self, pgtx: &Transaction<'_>, stamped_batch: &StampedData<Self::B>) {
        let batch = &stamped_batch.data;

        // Insert new record *before* applying patch
        supply::insert(pgtx, &batch.supply).await;
        // Apply patch *after* inserting new record
        if !batch.supply_patch.is_empty() {
            supply::patch_deposits(pgtx, &batch.supply_patch).await;
        }

        // New deposit addresses
        deposit_addresses::insert_many(pgtx, &batch.deposit_addresses).await;

        // Deposit conflicts
        for conflict in &batch.deposit_conflicts {
            // Retrieve height at which this address was first spotted as a deposit.
            let address_spot_height = match conflict {
                // Inter-block conflicts have always been spotted as a deposit in an earlier block
                DepositAddressConflict::Inter(conflict) => {
                    deposit_addresses::get_one(pgtx, conflict.address_id)
                        .await
                        .spot_height
                }
                // Intra-block confllicts are always spotted in current block
                DepositAddressConflict::Intra(_) => stamped_batch.height,
            };
            let record = &conflict.to_record(address_spot_height);
            deposit_addresses::delete_one(pgtx, record.address_id).await;
            deposit_conflicts::insert(pgtx, &record).await;
        }
    }

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header) {
        tracing::debug!("rolling back block {}", header.height);

        // for any deposits spotted at h --> query supply diffs
        // same for conflicts spotted at h
        // apply inverse patch.
        // not an issue if upstream rolled back even more rollback events will
        // eventually make it to this worker and delete any stale records.

        // Delete supply record of rolled back block
        supply::delete_at(pgtx, header.height).await;

        // Delete deposit records spotted in rolled back block
        deposit_addresses::delete_spotted_at(pgtx, header.height).await;

        // Restore deleted conflicts as deposits (if their address spot height < than h)
        let deposits_to_restore: Vec<DepositAddressRecord> =
            deposit_conflicts::get_conflicted_at(pgtx, header.height)
                .await
                .into_iter()
                // ignore intra-block conflicts
                .filter(|c| c.deposit_spot_height != header.height)
                .map(|c| c.into())
                .collect();
        deposit_addresses::insert_many(pgtx, &deposits_to_restore).await;

        // Delete conflict records spotted in rolled back block
        deposit_conflicts::delete_conflicted_at(pgtx, header.height).await;

        // Apply supply patch if one was prepared
        if let Some(ref patch) = self.rollback_patch {
            supply::patch_deposits(pgtx, patch).await;
        }
    }
}

impl PatchableStore for SpecStore {
    type P = Vec<SupplyDiff>;

    fn stage_rollback_patch(&mut self, patch: Self::P) {
        if patch.is_empty() {
            self.rollback_patch = None;
        } else {
            self.rollback_patch = Some(patch);
        }
    }
}

pub(super) async fn load_parser_cache(client: &Client) -> ParserCache {
    let supply = supply::get_latest(client).await.unwrap_or(SupplyRecord {
        height: -1,
        main: 0,
        deposits: 0,
    });
    let main_addresses = main_addresses::map_all(client).await;
    let deposit_addresses = deposit_addresses::map_all(client).await;
    let deposit_conflicts = deposit_conflicts::map_all(client).await;
    let deposit_ignored = deposit_ignored::get_all(client).await;

    ParserCache {
        supply,
        main_addresses,
        deposit_addresses,
        deposit_conflicts,
        deposit_ignored,
    }
}

impl Store {
    pub(super) async fn get_deposit_addresses_spotted_at(&self, height: Height) -> Vec<AddressID> {
        deposit_addresses::get_spotted_at(self.get_client(), height).await
    }

    pub(super) async fn get_deposit_conflicts_spotted_at(
        &self,
        height: Height,
    ) -> Vec<DepositAddressConflictRecord> {
        deposit_conflicts::get_conflicted_at(self.get_client(), height).await
    }
}

pub(super) mod migrations {

    use super::super::types::ExchangeID;
    use super::super::types::MainAddressRecord;
    use super::exchanges;
    use crate::core::types::AddressID;
    use crate::core::types::Height;
    use crate::framework::store::Migration;
    use crate::framework::store::MigrationEffect;
    use crate::framework::store::Revision;
    use crate::workers::exchanges::types::ExchangeRecord;
    use async_trait::async_trait;
    use tokio_postgres::Transaction;

    use super::{deposit_addresses, main_addresses, supply};

    const COINEX: ExchangeID = 1;
    const KUCOIN: ExchangeID = 3;
    const XEGGEX: ExchangeID = 7;

    /// Migration for revision 1.1
    #[derive(Debug)]
    pub struct Mig1_1 {}

    #[async_trait]
    impl Migration for Mig1_1 {
        fn description(&self) -> &'static str {
            "Adding 8 new coinex addresses"
        }

        fn revision(&self) -> Revision {
            Revision::new(1, 1)
        }

        async fn run(&self, pgtx: &Transaction<'_>) -> MigrationEffect {
            // Get current store height from last supply record
            let pre_mig_height = supply::get_latest(pgtx).await.map(|r| r.height);

            let new_main_addresses = vec![
                MainAddressRecord::new(
                    AddressID(9075411),
                    COINEX,
                    "9fNqgokdacnYykMZmtqjTnCbBJG9mhkifghV6Pmn6taBihUoG33",
                ),
                MainAddressRecord::new(
                    AddressID(9081471),
                    COINEX,
                    "9f3iGnXcebxzv4avYCUTt6dekgPMV1t5hpHdcJ4mAfX94yAiGFv",
                ),
                MainAddressRecord::new(
                    AddressID(9081481),
                    COINEX,
                    "9gPwnhhzc2tEkZHpcUeQ9J9wQWKzoLjCRbNx4qAsm2dK2RvVvib",
                ),
                MainAddressRecord::new(
                    AddressID(9081701),
                    COINEX,
                    "9gLxihuwAPkkgTpctawTRz82XVm46z6o8q3vMXVeZtyq6qtruWk",
                ),
                MainAddressRecord::new(
                    AddressID(9081711),
                    COINEX,
                    "9f65ghY8F5k7uKMACX7o2GfaV4EzWpsNW3gBTc23pAqTon8n7pE",
                ),
                MainAddressRecord::new(
                    AddressID(9120481),
                    COINEX,
                    "9iL1tEz6ENBLtaiMaEppnsrj9HjvnRaLdRyqiBPeCW6SyUtEaxM",
                ),
                MainAddressRecord::new(
                    AddressID(9186521),
                    COINEX,
                    "9h4WD9zRk7efQYM9jUYy3hrReJmYMYGen4yVeEw1SWGTuM6XNXv",
                ),
                MainAddressRecord::new(
                    AddressID(9213581),
                    COINEX,
                    "9fTyfkM1NWdBJvpM3rAq2SNdJ9yfvUqjM3MBStNS2Aq5fGE9Jgg",
                ),
            ];

            for record in new_main_addresses {
                add_main_address(pgtx, record).await;
            }

            // Get new store height from last supply record
            let post_mig_height = supply::get_latest(pgtx).await.map(|r| r.height);

            // Determine migration effect to return
            if post_mig_height == pre_mig_height {
                MigrationEffect::None
            } else {
                match post_mig_height {
                    Some(h) => MigrationEffect::Trimmed(h),
                    None => MigrationEffect::Reset,
                }
            }
        }
    }

    /// Migration for revision 1.2
    #[derive(Debug)]
    pub struct Mig1_2 {}

    #[async_trait]

    impl Migration for Mig1_2 {
        fn description(&self) -> &'static str {
            "Adding 2 new kucoin addresses"
        }

        fn revision(&self) -> Revision {
            Revision::new(1, 2)
        }

        async fn run(&self, pgtx: &Transaction<'_>) -> MigrationEffect {
            // Get current store height from last supply record
            let pre_mig_height = supply::get_latest(pgtx).await.map(|r| r.height);

            let new_main_addresses = vec![
                MainAddressRecord::new(
                    AddressID(8834201),
                    KUCOIN,
                    "9how9k2dp67jXDnCM6TeRPKtQrToCs5MYL2JoSgyGHLXm1eHxWs",
                ),
                MainAddressRecord::new(
                    AddressID(8834481),
                    KUCOIN,
                    "9fpUtN7d22jS3cMWeZxBbzkdnHCB46YRJ8qiiaVo2wRCkaBar1Z",
                ),
            ];

            for record in new_main_addresses {
                add_main_address(pgtx, record).await;
            }

            // Get new store height from last supply record
            let post_mig_height = supply::get_latest(pgtx).await.map(|r| r.height);

            // Determine migration effect to return
            if post_mig_height == pre_mig_height {
                MigrationEffect::None
            } else {
                match post_mig_height {
                    Some(h) => MigrationEffect::Trimmed(h),
                    None => MigrationEffect::Reset,
                }
            }
        }
    }

    /// Migration for revision 1.3
    #[derive(Debug)]
    pub struct Mig1_3 {}

    #[async_trait]
    impl Migration for Mig1_3 {
        fn description(&self) -> &'static str {
            "Adding xeggex to tracked exchanges"
        }

        fn revision(&self) -> Revision {
            Revision::new(1, 3)
        }

        async fn run(&self, pgtx: &Transaction<'_>) -> MigrationEffect {
            // Get current store height from last supply record
            let pre_mig_height = supply::get_latest(pgtx).await.map(|r| r.height);

            // Insert new exchange
            exchanges::insert(
                pgtx,
                &ExchangeRecord {
                    id: XEGGEX,
                    text_id: "xeggex".to_owned(),
                    name: "Xeggex".to_owned(),
                },
            )
            .await;

            // Main exchange address
            add_main_address(
                pgtx,
                MainAddressRecord::new(
                    AddressID(9336381),
                    XEGGEX,
                    "9hphYTmicjazd45pz2ovoHVPz5LTq9EvXoEK9JMGsfWuMtX6eDu",
                ),
            )
            .await;

            // Get new store height from last supply record
            let post_mig_height = supply::get_latest(pgtx).await.map(|r| r.height);

            // Determine migration effect to return
            if post_mig_height == pre_mig_height {
                MigrationEffect::None
            } else {
                match post_mig_height {
                    Some(h) => MigrationEffect::Trimmed(h),
                    None => MigrationEffect::Reset,
                }
            }
        }
    }

    /// Migration for revision 1.4
    #[derive(Debug)]
    pub struct Mig1_4 {}

    #[async_trait]
    impl Migration for Mig1_4 {
        fn description(&self) -> &'static str {
            "Adding new Coinex address"
        }

        fn revision(&self) -> Revision {
            Revision::new(1, 4)
        }

        async fn run(&self, pgtx: &Transaction<'_>) -> MigrationEffect {
            // Get current store height from last supply record
            let pre_mig_height = supply::get_latest(pgtx).await.map(|r| r.height);

            // Main exchange address
            add_main_address(
                pgtx,
                MainAddressRecord::new(
                    AddressID(9356241),
                    COINEX,
                    "9haE48wKvgYzc3WdBXRU9ERw2ZWWkGzJT8jGHcXvzQggftiQQdC",
                ),
            )
            .await;

            // Get new store height from last supply record
            let post_mig_height = supply::get_latest(pgtx).await.map(|r| r.height);

            // Determine migration effect to return
            if post_mig_height == pre_mig_height {
                MigrationEffect::None
            } else {
                match post_mig_height {
                    Some(h) => MigrationEffect::Trimmed(h),
                    None => MigrationEffect::Reset,
                }
            }
        }
    }

    /// Adds a new main address for an axistng exchange.
    ///
    /// Rolls back store to start of new address transactions.
    /// Returns height of first tx involving new main address.
    async fn add_main_address(pgtx: &Transaction<'_>, record: MainAddressRecord) -> Option<Height> {
        tracing::trace!("add_main_address {record:?}");

        // Insert new main address
        main_addresses::insert(pgtx, &record).await;

        // Delete address from known deposit addresses,
        // just in case it got falsely registered as such.
        deposit_addresses::delete_one(pgtx, record.address_id).await;

        // Determine how far data needs to be rolled back by getting
        // height of first tx involving the new main address.
        let first_tx_height = get_first_tx_height(pgtx, record.address_id).await;

        // Rollback supply if new address is already in use
        if let Some(h) = first_tx_height {
            supply::delete_from(pgtx, h).await;
        }

        first_tx_height
    }

    /// Retrieve height of first tx involving given `address_id`.
    ///
    /// Reads upstream store (erg_diffs), which is guaranteed to be
    /// at same height, at least.
    async fn get_first_tx_height(pgtx: &Transaction<'_>, address_id: AddressID) -> Option<Height> {
        tracing::trace!("get_first_tx_height {address_id:?}");
        let sql = "select min(height) from erg.balance_diffs where address_id = $1;";
        pgtx.query_one(sql, &[&address_id])
            .await
            .map(|row| row.get::<usize, Option<Height>>(0))
            .unwrap()
    }
}

mod service;
mod store;
pub mod types;

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

use crate::config::PostgresConfig;
use crate::core::types::CoreData;
use crate::core::types::Header;
use crate::core::types::Height;
use crate::core::types::Timestamp;
use crate::framework::EventHandler;
use crate::framework::EventHandling;
use crate::framework::Source;
use crate::framework::StampedData;
use crate::monitor::MonitorMessage;
use service::CoingeckoService;
use store::Store;
use types::Batch;
use types::BlockRecord;
use types::HourlyRecord;
use types::ProvisionalBlockRecord;

const WORKER_ID: &'static str = "coingecko";

const TEN_SECONDS: tokio::time::Duration = Duration::from_secs(10);

type SharedCache = Arc<RwLock<Cache>>;
type SharedStore = Arc<Mutex<Store>>;

pub struct Worker {
    tracker: Tracker,
    event_handler: EventHandler<Workflow>,
}

impl Worker {
    pub async fn new(
        pgconf: &PostgresConfig,
        source: &mut impl Source<S = CoreData>,
        monitor_tx: Sender<MonitorMessage>,
        custom_coingecko_url: Option<&str>,
    ) -> Self {
        let store = Store::new(pgconf, &store::SCHEMA).await;
        // Seed hourly data
        store.seed_hourly_data().await;
        // Then loading the cache
        let cache = store.load_cache().await;
        let header = store.get_header().clone();
        // Wrap store and cache to be shared between tracker and event handler's workflow
        let store = SharedStore::new(Mutex::new(store));
        let cache = SharedCache::new(RwLock::new(cache));
        let workflow = Workflow {
            header,
            cache: cache.clone(),
            store: store.clone(),
        };
        Self {
            tracker: Tracker::new(cache, store, custom_coingecko_url),
            event_handler: EventHandler::new_with("coingecko", workflow, source, monitor_tx).await,
        }
    }

    #[tracing::instrument(name = "coingecko", skip_all)]
    pub async fn start(&mut self) {
        let mut throttle = false;
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    tracing::info!("got a ctrl-c message");
                    return;
                },
                _ = tokio::time::sleep(TEN_SECONDS), if throttle => {
                    tracing::trace!("throttling off");
                    throttle = false;
                }
                _ = self.tracker.time_to_sync(), if !throttle => {
                    match self.tracker.poll().await {
                        Some(data) => {
                            self.tracker.handle(data).await
                        },
                        None => {
                            // Coingecko is down or we're too early
                            tracing::trace!("throttling on");
                            throttle = true;
                        }
                    }
                },
                event = self.event_handler.recv() => {
                    self.event_handler.process_upstream_event(&event.unwrap()).await;
                }
            }
        }
    }
}

pub struct Cache {
    pub recent_hourly_records: Vec<HourlyRecord>,
    pub provisional_records: Vec<ProvisionalBlockRecord>,
}

impl Cache {
    /// Removes unneeded hourly records.
    ///
    /// Only keeps hourly records needed to interpolate provisional records.
    /// This is achieved by deleting all hourly records prior to the first
    /// provisional record, except for the most recent one of them.
    pub fn trim_hourly_records(&mut self) {
        // Determine timestamp of first hourly record to keep.
        let since = match self.provisional_records.first() {
            Some(first_provisional_record) => self
                .recent_hourly_records
                .iter()
                .filter(|hr| hr.timestamp <= first_provisional_record.timestamp)
                .map(|hr| hr.timestamp)
                .max()
                // Keep all hourly records if none are prior to first provisional one.
                .unwrap_or(0),
            // Keep all hourly records if there are no provisional records yet.
            None => 0,
        };
        self.recent_hourly_records
            .retain(|hr| hr.timestamp >= since);
    }
}

pub struct Workflow {
    // Header the store is currently at.
    // Purely to be able to implement the EventHandling trait which has
    // an non async header() method, so we can't get the header from
    // the store as usual (because it is behind an async mutex).
    header: Header,
    cache: SharedCache,
    store: SharedStore,
}

#[async_trait]
impl EventHandling for Workflow {
    type U = CoreData;
    type D = ();

    /// Create and initialize a new event handling workflow.
    async fn new(_pgconf: &PostgresConfig) -> Self {
        todo!("not needed") // See todo for EventHandler::new_with()
    }

    /// Process new block data.
    #[tracing::instrument(skip_all, level=tracing::Level::TRACE)]
    async fn include_block(&mut self, data: &StampedData<CoreData>) {
        tracing::trace!("include_block {}", data.height);
        let mut cache = self.cache.write().await;

        let batch = prepare_batch(data.height, data.timestamp, &cache.recent_hourly_records);
        let stamped_batch = data.wrap(batch);
        self.store.lock().await.persist(&stamped_batch).await;

        // Update cache
        if let Some(ref pr) = stamped_batch.data.provisional_block_record {
            cache.provisional_records.push(pr.clone());
        }
        cache.trim_hourly_records();

        // Update header
        self.header = self.store.lock().await.get_header().clone();
    }

    /// Roll back a block and return previous head.
    #[tracing::instrument(skip_all, level=tracing::Level::TRACE)]
    async fn roll_back(&mut self, height: Height) -> Header {
        let mut store = self.store.lock().await;
        store.roll_back(height).await;

        // Update cache
        let mut cache = self.cache.write().await;
        cache.provisional_records.retain(|pr| pr.height != height);

        let header = store.get_header().clone();
        self.header = header.clone();
        header
    }

    /// Get last processed header.
    fn header<'a>(&'a self) -> &'a Header {
        &self.header
    }
}

fn prepare_batch(
    height: Height,
    timestamp: Timestamp,
    hourly_records: &Vec<HourlyRecord>,
) -> Batch {
    // Hard-coded value for first few blocks prior to first Coingecko datapoint.
    if height <= 3 {
        return Batch {
            block_record: BlockRecord::new(height, HourlyRecord::genesis().usd),
            provisional_block_record: None,
        };
    }

    let last_hourly_record = hourly_records.last().expect("always some records");

    for w in hourly_records.windows(2) {
        tracing::trace!("w: {w:?}");
        if timestamp >= w[0].timestamp && timestamp <= w[1].timestamp {
            let usd = interpolate(timestamp, &w[0], &w[1]);
            return Batch {
                block_record: BlockRecord::new(height, usd),
                provisional_block_record: None,
            };
        }
    }

    Batch {
        block_record: BlockRecord::new(height, last_hourly_record.usd),
        provisional_block_record: Some(ProvisionalBlockRecord { timestamp, height }),
    }
}

struct Tracker {
    cache: SharedCache,
    store: SharedStore,
    service: CoingeckoService,
}

impl Tracker {
    pub fn new(cache: SharedCache, store: SharedStore, custom_url: Option<&str>) -> Self {
        let coingecko_url =
            custom_url.unwrap_or("https://api.coingecko.com/api/v3/coins/ergo/market_chart/range");
        Self {
            cache,
            store,
            service: CoingeckoService::new(coingecko_url),
        }
    }

    async fn poll(&self) -> Option<Vec<HourlyRecord>> {
        let since_ms = self
            .cache
            .read()
            .await
            .recent_hourly_records
            .last()
            .expect("always some records")
            .timestamp;

        match self.service.fetch_since(since_ms).await {
            Ok(hourly_records) => {
                tracing::info!("fetched {} new records", hourly_records.len());
                if hourly_records.is_empty() {
                    None
                } else {
                    Some(hourly_records)
                }
            }
            Err(e) => {
                // CoinGecko may be down. That's fine.
                tracing::warn!("could not retrieve CoinGecko data. Error was: {:?}", e);
                None
            }
        }
    }

    /// Process new hourly datapoints.
    ///
    /// Saves hourly records to db and interpolates block records.
    #[tracing::instrument(skip_all, level=tracing::Level::INFO)]
    async fn handle(&self, hourly_records: Vec<HourlyRecord>) {
        let mut cache = self.cache.write().await;

        // Latest available hourly timestamp
        let last_hourly_timestamp = hourly_records.last().unwrap().timestamp;

        // Prepare an iteratable over updateable block records (the ones prior to or on latest hourly datapoint)
        let mut updatable_block_records = cache
            .provisional_records
            .iter()
            .filter(|pr| pr.timestamp <= last_hourly_timestamp);

        // Progress through updateable records and containing hourly datapoints
        let mut updates: Vec<BlockRecord> = vec![];
        // Loop through pairs of hourly records
        for w in hourly_records.windows(2) {
            match updatable_block_records.next() {
                Some(pr) => {
                    if pr.timestamp > w[1].timestamp {
                        // Current block is outside of current hourly window, move on to next window
                        continue;
                    }
                    updates.push(BlockRecord::new(
                        pr.height,
                        interpolate(pr.timestamp, &w[0], &w[1]),
                    ));
                }
                None => break,
            }
        }

        // Apply changes to store
        self.store
            .lock()
            .await
            .persist_tracker_data(&hourly_records, &updates)
            .await;

        // Update cache
        // Remove interpolated block records from provisional cache
        let last_interpolated_height = updates.last().and_then(|tbr| Some(tbr.height)).unwrap_or(0);
        cache
            .provisional_records
            .retain(|pr| pr.height > last_interpolated_height);
        // Append new hourly records
        cache.recent_hourly_records.extend(hourly_records);
        // Remove unneeded hourly records
        cache.trim_hourly_records();
    }

    /// Sleeps untill new data is expected to be available
    pub async fn time_to_sync(&self) {
        loop {
            if self.needs_syncing().await {
                break;
            }
            tracing::trace!("waiting until until time to sync");
            tokio::time::sleep(TEN_SECONDS).await;
        }
    }

    /// Is it time to get more data from Coingecko?
    async fn needs_syncing(&self) -> bool {
        let cache = self.cache.read().await;
        // We don't want to fetch all data at once during initial sync,
        // so we only fetch when there's 12 hours of data left ahead.
        cache.recent_hourly_records.len() <= 12 && {
            // Fetch only once new records are expected to be available
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                * 1000;
            let last_ms = cache.recent_hourly_records.last().unwrap().timestamp;
            now_ms > last_ms && now_ms - last_ms > 3600_000
        }
    }
}

/// Returns value interpolated at given `t` between two hourly records.
fn interpolate(t: Timestamp, r0: &HourlyRecord, r1: &HourlyRecord) -> f32 {
    tracing::trace!("interpolate {t} {r0:?} {r1:?}");
    assert!(t >= r0.timestamp);
    assert!(t <= r1.timestamp);
    let weight = (t - r0.timestamp) as f32 / (r1.timestamp - r0.timestamp) as f32;
    r0.usd + (r1.usd - r0.usd) * weight
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolate_between() {
        let h0 = HourlyRecord {
            timestamp: 1704956400000,
            usd: 1.5,
        };
        let h1 = HourlyRecord {
            timestamp: 1704960000000,
            usd: 1.7,
        };
        let t: Timestamp = h0.timestamp + 24 * 60 * 1000; // + 24 minutes
        assert_eq!(h1.timestamp - h0.timestamp, 3_600_000);
        let usd = interpolate(t, &h0, &h1);
        assert_eq!(usd, 1.58);
    }

    #[test]
    fn interpolate_on_lower_bound() {
        let h0 = HourlyRecord {
            timestamp: 1704956400000,
            usd: 1.5,
        };
        let h1 = HourlyRecord {
            timestamp: 1704960000000,
            usd: 1.7,
        };
        let t: Timestamp = h0.timestamp;
        assert_eq!(h1.timestamp - h0.timestamp, 3_600_000);
        let usd = interpolate(t, &h0, &h1);
        assert_eq!(usd, h0.usd);
    }

    #[test]
    fn interpolate_on_upper_bound() {
        let h0 = HourlyRecord {
            timestamp: 1704956400000,
            usd: 1.5,
        };
        let h1 = HourlyRecord {
            timestamp: 1704960000000,
            usd: 1.7,
        };
        let t: Timestamp = h1.timestamp;
        assert_eq!(h1.timestamp - h0.timestamp, 3_600_000);
        let usd = interpolate(t, &h0, &h1);
        assert_eq!(usd, h1.usd);
    }

    #[test]
    fn cache_trim_hourlies_no_change_when_no_provisionals() {
        let mut cache = Cache {
            recent_hourly_records: vec![
                HourlyRecord::new(1000, 1.0),
                HourlyRecord::new(2000, 2.0),
                HourlyRecord::new(3000, 3.0),
            ],
            provisional_records: vec![],
        };
        cache.trim_hourly_records();
        assert_eq!(cache.recent_hourly_records.len(), 3);
    }

    #[test]
    fn cache_trim_hourlies_keeps_all_but_last_prior_to_first_provisionals() {
        let mut cache = Cache {
            recent_hourly_records: vec![
                HourlyRecord::new(1000, 1.0),
                HourlyRecord::new(2000, 2.0),
                HourlyRecord::new(3000, 3.0),
                HourlyRecord::new(4000, 4.0),
            ],
            provisional_records: vec![
                ProvisionalBlockRecord {
                    timestamp: 3500,
                    height: 100,
                },
                ProvisionalBlockRecord {
                    timestamp: 4500,
                    height: 101,
                },
            ],
        };
        cache.trim_hourly_records();
        assert_eq!(
            cache.recent_hourly_records,
            vec![HourlyRecord::new(3000, 3.0), HourlyRecord::new(4000, 4.0),]
        );
    }

    #[test]
    fn cache_trim_hourlies_keeps_all_from_one_on_first_provisionals() {
        let mut cache = Cache {
            recent_hourly_records: vec![
                HourlyRecord::new(1000, 1.0),
                HourlyRecord::new(2000, 2.0),
                HourlyRecord::new(3000, 3.0),
                HourlyRecord::new(4000, 4.0),
            ],
            provisional_records: vec![
                ProvisionalBlockRecord {
                    timestamp: 3000,
                    height: 100,
                },
                ProvisionalBlockRecord {
                    timestamp: 4500,
                    height: 101,
                },
            ],
        };
        cache.trim_hourly_records();
        assert_eq!(
            cache.recent_hourly_records,
            vec![HourlyRecord::new(3000, 3.0), HourlyRecord::new(4000, 4.0),]
        );
    }

    #[test]
    fn cache_trim_hourlies_keeps_last_when_all_prior_to_provisionals() {
        let mut cache = Cache {
            recent_hourly_records: vec![
                HourlyRecord::new(1000, 1.0),
                HourlyRecord::new(2000, 2.0),
                HourlyRecord::new(3000, 3.0),
                HourlyRecord::new(4000, 4.0),
            ],
            provisional_records: vec![ProvisionalBlockRecord {
                timestamp: 5000,
                height: 100,
            }],
        };
        cache.trim_hourly_records();
        assert_eq!(
            cache.recent_hourly_records,
            vec![HourlyRecord::new(4000, 4.0),]
        );
    }
}

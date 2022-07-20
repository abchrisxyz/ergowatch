use anyhow::Error;
use anyhow::Result;
use log::info;
use reqwest;
use serde::Deserialize;

// CoinGecko market_chart returns hourly data for time windows up to 90 days.
const MAX_TIMESPAN_SECS: u64 = 86400 * 90;

const TWO_DAYS_SECS: u64 = 86400 * 2;

pub struct CoingeckoService {
    url: String,
    interval: f32,
}

#[derive(Deserialize, Debug)]
struct Response {
    prices: Vec<(u64, f64)>,
}

pub type TimeSeries = Vec<PricePoint>;

#[derive(Debug, PartialEq)]
pub struct PricePoint {
    pub timestamp_ms: u64,
    pub usd: f64,
}

impl CoingeckoService {
    /// New CoinGecko API service
    pub fn new(url: String, interval: f32) -> Self {
        Self {
            url: url,
            interval: interval,
        }
    }

    /// Fetch ERG/USD price since `since` till now.
    /// Timestamps in milliseconds.
    pub fn fetch_since(&self, since_ms: u64) -> Result<TimeSeries, Error> {
        let since = since_ms / 1000;
        info!("Fetching CoinGecko data since {}", since);
        let now = now();
        assert_eq!(since < now, true);

        let mut timeseries: TimeSeries = vec![];
        for from in (since..now).step_by(MAX_TIMESPAN_SECS as usize) {
            // Ensure time window is larger than 2 days, to trigger hourly data.
            let fr = if now - from < TWO_DAYS_SECS {
                from - TWO_DAYS_SECS
            } else {
                from
            };
            let to = std::cmp::min(fr + MAX_TIMESPAN_SECS, now);

            // Current last timestamp
            let last_ms = match timeseries.last() {
                Some(p) => p.timestamp_ms,
                None => since_ms,
            };

            // Fetch data and filter out any timestamps earlier than current last one
            let mut series: TimeSeries = self
                .fetch_range(fr, to)?
                .into_iter()
                .filter(|p| p.timestamp_ms > last_ms)
                .collect();

            timeseries.append(&mut series);

            // CoinGecko rate limitting
            std::thread::sleep(std::time::Duration::from_secs_f32(self.interval));
        }

        Ok(timeseries)
    }

    /// Fetch ERG/USD price for `fr`-`to` time range.__rust_force_expr!
    /// Timestamps in milliseconds.
    fn fetch_range(&self, fr: u64, to: u64) -> Result<TimeSeries, Error> {
        info!("Querying range {} - {}", fr, to);
        assert_eq!(fr < to, true);
        assert_eq!(to - fr <= MAX_TIMESPAN_SECS, true);
        let qry = format!("{}?vs_currency=usd&from={}&to={}", &self.url, fr, to);
        let res: Response = reqwest::blocking::get(&qry)?.json()?;
        Ok(res.to_timeseries())
    }
}

impl Response {
    fn to_timeseries(&self) -> TimeSeries {
        self.prices
            .iter()
            .map(|(t, v)| PricePoint {
                timestamp_ms: *t,
                usd: *v,
            })
            .collect()
    }
}

/// Current timestamp in seconds
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::PricePoint;
    use super::Response;
    use pretty_assertions::assert_eq;

    #[test]
    fn response_to_timeseries() {
        let response = Response {
            prices: vec![
                (1577790749234, 0.48306243229959933),
                (1577794460300, 0.4376726048392734),
                (1577798055240, 0.501573336936612),
            ],
        };
        let timeseries = response.to_timeseries();
        assert_eq!(timeseries.len(), 3);
        assert_eq!(
            timeseries[0],
            PricePoint {
                timestamp_ms: 1577790749234,
                usd: 0.48306243229959933
            }
        );
        assert_eq!(
            timeseries[1],
            PricePoint {
                timestamp_ms: 1577794460300,
                usd: 0.4376726048392734
            }
        );
        assert_eq!(
            timeseries[2],
            PricePoint {
                timestamp_ms: 1577798055240,
                usd: 0.501573336936612
            }
        );
    }
}

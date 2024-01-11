use reqwest;
use serde::Deserialize;

use super::types::HourlyRecord;
use super::types::MilliSeconds;

type TimeSeries = Vec<HourlyRecord>;

// CoinGecko market_chart returns hourly data for time windows between 2 to 90 days.
const NB_DAYS: i64 = 3;
const MAX_TIMESPAN_SECS: Seconds = Seconds(86400 * NB_DAYS);
const MAX_TIMESPAN_MS: MilliSeconds = MAX_TIMESPAN_SECS.0 * 1000;

#[derive(Debug, PartialEq, PartialOrd)]
struct Seconds(i64);

impl From<MilliSeconds> for Seconds {
    fn from(value: MilliSeconds) -> Self {
        Self(value / 1000)
    }
}

pub struct CoingeckoService {
    url: String,
}

#[derive(Deserialize, Debug)]
/// API response with timestamps in ms
struct Response {
    prices: Vec<(i64, f32)>,
}

impl CoingeckoService {
    /// New CoinGecko API service
    pub fn new(url: &str) -> Self {
        tracing::debug!("using coingecko api: {}", url);
        Self {
            url: url.to_owned(),
        }
    }

    /// Fetch next 3 days of ERG/USD price since `since_ms`.
    ///
    /// Timestamps in milliseconds.
    pub async fn fetch_since(&self, since: MilliSeconds) -> Result<TimeSeries, String> {
        tracing::info!("Fetching CoinGecko data since {:?}", since);
        let now = now();
        assert_eq!(since < now, true);

        // Ensure time window is larger than 2 days, to trigger hourly data.
        let fr = since;
        let to = fr + MAX_TIMESPAN_MS;

        // Fetch data and filter out any timestamps earlier than current last one
        let timeseries = self
            .fetch_range(Seconds::from(fr), Seconds::from(to))
            .await?
            .into_iter()
            .filter(|p| p.timestamp > since)
            .collect();

        Ok(timeseries)
    }

    /// Fetch ERG/USD price for `fr`-`to` time range.
    /// Timestamps in seconds.
    async fn fetch_range(&self, fr: Seconds, to: Seconds) -> Result<TimeSeries, String> {
        tracing::info!("Querying range {fr:?} - {to:?}");
        assert_eq!(fr < to, true);
        assert_eq!(Seconds(to.0 - fr.0) <= MAX_TIMESPAN_SECS, true);
        let qry = format!("{}?vs_currency=usd&from={}&to={}", &self.url, fr.0, to.0);
        let res = match reqwest::get(&qry).await {
            Ok(response) => response,
            Err(e) => {
                return Err(e.to_string());
            }
        };
        res.json::<Response>()
            .await
            .map(|data| data.to_timeseries())
            .map_err(|e| e.to_string())
    }
}

impl Response {
    fn to_timeseries(&self) -> TimeSeries {
        self.prices
            .iter()
            .map(|(t, v)| HourlyRecord {
                timestamp: *t,
                usd: *v,
            })
            .collect()
    }
}

/// Current timestamp in ms
fn now() -> MilliSeconds {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        * 1000
}

#[cfg(test)]
mod tests {
    use super::HourlyRecord;
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
            HourlyRecord {
                timestamp: 1577790749234,
                usd: 0.48306243229959933
            }
        );
        assert_eq!(
            timeseries[1],
            HourlyRecord {
                timestamp: 1577794460300,
                usd: 0.4376726048392734
            }
        );
        assert_eq!(
            timeseries[2],
            HourlyRecord {
                timestamp: 1577798055240,
                usd: 0.501573336936612
            }
        );
    }
}

# Changelog

## v0.5.7 - 2023-05-21

### Fixed
- [Issue 75](https://github.com/abchrisxyz/ergowatch/issues/75) - Add new main Kucoin address
- [Issue 76](https://github.com/abchrisxyz/ergowatch/issues/76) - Fix issue with multiple deposit address conflicts
---

## v0.5.6 - 2023-04-09

### Fixed
- Handle duplicate data-inputs
---

## v0.5.5 - 2023-04-03

### Fixed
- Remove default values from FastAPI path parameters
---

## v0.5.4 - 2023-01-31

### Fixed
- Handle invalid UTF-8 bytes during EIP-4 token rendering
---

## v0.5.3 - 2023-01-24

### Fixed
- Disable age calculation in fork handling until [Issue 73](https://github.com/abchrisxyz/ergowatch/issues/73) is resolved
---

## v0.5.2 - 2022-12-23

### New Features
- Added `ergusd` flag to `/metrics` api to include ERG/USD data.
- Added relative change summary for supply distribution and composition.
- Added `/metrics/overview` endpoint.

### Changed
- Increased metrics time window limit to 2000 records.
- Simplified deposit tracking:
    - no more dedicated repair thread
    - dropping `/repairs` endpoint introduced in 0.5.1
    - requires some changes in config file.
- Hiding supply age from api docs until [Issue 73](https://github.com/abchrisxyz/ergowatch/issues/73) is resolved

### Fixed
- Fixed stale ERG/USD cache value.
- Fixed early supply age records with unhandled zero timestamps.
- Fixed contracts supply age bias.
- Made `/metrics/exchanges/supply` behave as other `/metrics` endpoints.
---

## v0.5.1 - 2022-11-24

### New Features
- Added `/repairs` endpoint exposing repair sessions info

### Changed
- Removed hard-coded API response CORS headers. 
---

## v0.5.0 - 2022-11-02

BREAKING CHANGES - requires a full db resync

### New features

- Added -r option to resume interrupted repair sessions.
- Tracking new exchanges (TradeOgre and Huobi)
- [Issue 38](https://github.com/abchrisxyz/ergowatch/issues/38) - Sync CoinGecko ERG/USD data
- [Issue 43](https://github.com/abchrisxyz/ergowatch/issues/43) - Added supply age metrics
- [Issue 57](https://github.com/abchrisxyz/ergowatch/issues/57) - Track address level supply age (no api yet)
- [Issue 58](https://github.com/abchrisxyz/ergowatch/issues/58) - Added address counts by minimal balance
- [Issue 67](https://github.com/abchrisxyz/ergowatch/issues/67) - Added supply distribution metrics
- [Issue 68](https://github.com/abchrisxyz/ergowatch/issues/68) - Added transaction metrics
- [Issue 69](https://github.com/abchrisxyz/ergowatch/issues/69) - Added volume metrics
- Added supply composition metrics
- Added metrics summaries

### Changed

- Calls to `/metrics` api's now always include latest available record if time window spans latest timestamp, regardless of `r` value. See [time_windows.md](https://github.com/abchrisxyz/ergowatch/blob/master/api/src/api/routes/metrics/time_windows.md).

### Fixed
- Fixed panics on repair conflicts.
---

## v0.4.3 - 2022-09-26

### Fixed

- [Issue 70](https://github.com/abchrisxyz/ergowatch/issues/70) - Fixed cex address detection issue with airdrops
---

## v0.4.2 - 2022-09-01

### Fixed

- [Issue 65](https://github.com/abchrisxyz/ergowatch/issues/65) - Fixed division by zero during rollbacks
- Fixed deadlock when pausing repair events
---

## v0.4.1 - 2022-08-29

BREAKING CHANGES - requires a full db resync. Intermediate release to fix indexing issues.

### New features
- [Issue 55](https://github.com/abchrisxyz/ergowatch/issues/55) - Added height <--> timestamp conversion utils
- [Issue 57](https://github.com/abchrisxyz/ergowatch/issues/57) - Track address-level mean supply age

### Fixed
- [Issue 64](https://github.com/abchrisxyz/ergowatch/issues/64) - Work around b-tree index limitations
---

## v0.4.0 - 2022-07-08

BREAKING CHANGES - requires a full db resync

### New features
- [Issue 49](https://github.com/abchrisxyz/ergowatch/issues/49) - Block difficulty is stored to db.
- [Issue 50](https://github.com/abchrisxyz/ergowatch/issues/50) - Box sizes are stored to db.
- [Issue 51](https://github.com/abchrisxyz/ergowatch/issues/51) - Block extensions are stored to db.
- [Issue 52](https://github.com/abchrisxyz/ergowatch/issues/52) - Block votes are stored to db.

### Fixed

- Faster balance diff processing.
---

## v0.3.0 - 2022-06-13

### New features
- [Issue 15](https://github.com/abchrisxyz/ergowatch/issues/15) - Added rich lists endpoint.
- [Issue 28](https://github.com/abchrisxyz/ergowatch/issues/28) - Added exchange supply API's.
- [Issue 41](https://github.com/abchrisxyz/ergowatch/issues/41) - Added address labels endpoint.

### Changed
- [Issue 42](https://github.com/abchrisxyz/ergowatch/issues/42) - Response format of `/metrics` endpoints return flat arrays instead of record collections.

### Fixed
- [Issue 25](https://github.com/abchrisxyz/ergowatch/issues/25) - Fixed unhandled `SUnit` register values.
- [Issue 33](https://github.com/abchrisxyz/ergowatch/issues/33) - Fixed processing order of blocks with multiple candidates.
- [Issue 35](https://github.com/abchrisxyz/ergowatch/issues/35) - Fixed slow rollbacks.

### Housekeeping
- [Issue 34](https://github.com/abchrisxyz/ergowatch/issues/34) - Bootstrapping `work_mem` is now configurable.
- [Issue 26](https://github.com/abchrisxyz/ergowatch/issues/26) - Added terms of service.
---

## v0.2.2 - 2022-05-04

### Fixed
- [Issue 27](https://github.com/abchrisxyz/ergowatch/issues/27) - Handle non-consecutive duplicated assets.


## v0.2.1 - 2022-04-25

### Fixed
- [Issue 24](https://github.com/abchrisxyz/ergowatch/issues/24) - Prevent panics from register parsing.
---

## v0.2.0 - 2022-04-05

### New features
- [Issue 13](https://github.com/abchrisxyz/ergowatch/issues/13) - Made node poll interval configurable.
- [Issue 16](https://github.com/abchrisxyz/ergowatch/issues/17) - Added token details endpoint.
- [Issue 17](https://github.com/abchrisxyz/ergowatch/issues/17) - Added sync status API.
- [Issue 21](https://github.com/abchrisxyz/ergowatch/issues/21) - Added contracts supply endpoint.
- [Issue 22](https://github.com/abchrisxyz/ergowatch/issues/22) - Added utxo metrics

### Changed
- [Issue 19](https://github.com/abchrisxyz/ergowatch/issues/19) - Token supply endpoint breaks down circulating supply between P2PK and contract addresses.

### Fixed
- [Issue 11](https://github.com/abchrisxyz/ergowatch/issues/11) - Non-zero timestamp for genesis header.

### Housekeeping
- [Issue 12](https://github.com/abchrisxyz/ergowatch/issues/12) - Handle genesis boxes in bootstrapping process (not separately).
---

## v0.1.0 - 2022-03-22
🎉 first release


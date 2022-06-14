# Changelog


## v0.3.0 - 2022-06-13

### New features
[Issue 15](https://github.com/abchrisxyz/ergowatch/issues/15) - Added rich lists endpoint.

[Issue 28](https://github.com/abchrisxyz/ergowatch/issues/28) - Added exchange supply API's.

[Issue 41](https://github.com/abchrisxyz/ergowatch/issues/41) - Added address labels endpoint.

### Changed
[Issue 42](https://github.com/abchrisxyz/ergowatch/issues/42) - Response format of `/metrics` endpoints return flat arrays instead of record collections.

### Fixed
[Issue 25](https://github.com/abchrisxyz/ergowatch/issues/25) - Fixed unhandled `SUnit` register values.

[Issue 33](https://github.com/abchrisxyz/ergowatch/issues/33) - Fixed processing order of blocks with multiple candidates.

[Issue 35](https://github.com/abchrisxyz/ergowatch/issues/35) - Fixed slow rollbacks.

### Housekeeping
[Issue 34](https://github.com/abchrisxyz/ergowatch/issues/34) - Bootstrapping `work_mem` is now configurable.

[Issue 26](https://github.com/abchrisxyz/ergowatch/issues/26) - Added terms of service.


## v0.2.2 - 2022-05-04

### Fixed
[Issue 27](https://github.com/abchrisxyz/ergowatch/issues/27) - Handle non-consecutive duplicated assets.


## v0.2.1 - 2022-04-25

### Fixed
[Issue 24](https://github.com/abchrisxyz/ergowatch/issues/24) - Prevent panics from register parsing.


## v0.2.0 - 2022-04-05

### New features
[Issue 13](https://github.com/abchrisxyz/ergowatch/issues/13) - Made node poll interval configurable.

[Issue 16](https://github.com/abchrisxyz/ergowatch/issues/17) - Added token details endpoint.

[Issue 17](https://github.com/abchrisxyz/ergowatch/issues/17) - Added sync status API.

[Issue 21](https://github.com/abchrisxyz/ergowatch/issues/21) - Added contracts supply endpoint.

[Issue 22](https://github.com/abchrisxyz/ergowatch/issues/22) - Added utxo metrics

### Changed
[Issue 19](https://github.com/abchrisxyz/ergowatch/issues/19) - Token supply endpoint breaks down circulating supply between P2PK and contract addresses.

### Fixed
[Issue 11](https://github.com/abchrisxyz/ergowatch/issues/11) - Non-zero timestamp for genesis header.

### Housekeeping
[Issue 12](https://github.com/abchrisxyz/ergowatch/issues/12) - Handle genesis boxes in bootstrapping process (not separately).


## v0.1.0 - 2022-03-22
🎉 first release


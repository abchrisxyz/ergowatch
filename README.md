<p align="center" style="margin-bottom: 0px !important;">
  <img width="128" src="https://github.com/abchrisxyz/ergowatch-ui/raw/master/ewi/public/ew-logo.svg" alt="logo" align="center">
</p>

# ErgoWatch
Ergo blockchain stats & monitoring.

ErgoWatch consists of a chain indexer (the "watcher") and an API exposing indexed data.

API docs available at https://api.ergo.watch/docs.

If looking for the frontend of https://ergo.watch see https://github.com/abchrisxyz/ergowatch-ui.

Note that https://ergo.watch is currenlty running on an older backend (https://github.com/abchrisxyz/ergowatch/tree/explorer-based), soon to be replaced.

## Roadmap

- [x] Address counts, ERG/token balances and tokens supply/burnings

- [ ] Basic metrics (at least on par with old backend)

- [ ] V1 Oracle Pools (Integrating https://github.com/thedelphiproject/ergo-oracle-stats-backend)

- [ ] SigmaUSD

At this point, should be ready to retire old backend and migrate https://ergo.watch.

- [ ] V2 Oracle Pools

## Watcher

An indexer that queries a node's API and populates the database.

```
USAGE:
    watcher [OPTIONS]

OPTIONS:
    -c, --config <PATH>                          Path to config file
    -h, --help                                   Print help information
    -m, --allow-migrations                       Allow database migrations to be applied
    -v, --version                                Print version information
    -x, --exit                                   Exit once synced (mostly for integration tests)
```


### Requirements

The watcher needs a synced Ergo node and a PostgreSQL server to run with. As long as enough disk space is available, the watcher will run smoothly on most machines. As of now (block ~710k) the database is around 32GB.

> Note that the current implementation doesn't use streaming or any async features. Syncing from scratch with a node that is on another host will be **very slow**.

### Installation

#### Build

For a local build, install [Rust](https://www.rust-lang.org/tools/install) and then run `cargo build --release` from within the `watcher` directory. The binary will be located under `watcher/target/release`.

#### Test

Unit tests can be run with `cargo test`.

There is also a testbench performing a number of integration tests that need access to a Postgres server. Make sure you run `cargo build --release` before running the testbench. Refer to the README in the `testbench` directory for more details.

#### Database

The watcher expects a database with the schema defined in `db/schema.sql`. Constraints and indexes are set by the watcher when appropriate and should not be loaded beforehand.

### Usage

Typical usage is like so: `watcher -c <path/to/config.toml>`.

Run `watcher -h` or `watcher --help` for more options.

#### Configuration

Node url and database connection settings can be configured through a config file. See `watcher/config/default.toml` for an example.

Some config file settings can be overwritten through environment variables:

- `EW_LOG`: log level, one of `DEBUG`, `INFO`, `WARN`, `ERROR`
- `EW_DB_HOST`: Postgres host
- `EW_DB_PORT`: Postgres port
- `EW_DB_NAME`: Postgres name
- `EW_DB_USER`: Postgres user
- `EW_DB_PASS`: Postgres pass
- `EW_NODE_URL`: URL to Ergo node (including port, if any)

The `docker-compose.yml` might also be a good place to look at to see how things ought to be configured.

#### Initial Sync

When running for the first time (i.e. with an empty database), the watcher will first sync core tables only, then load database constraints and populate other tables. If interrupted during the bootstrap process, it is safe to restart the watcher, it'll pick up where it left off.

### Fork handling

The watcher only keeps main chain blocks. In the event of a chain fork, the old branch is rolled back up to the forking block and main chain blocks are included again from that point onwards.

## API

Docs: see https://ergo.watch/api/v0/docs.

### Test

```
cd api/src/tests
pip install -r requirements.txt
pytest
```

### Run

```
cd api/src
uvicorn main:app
```


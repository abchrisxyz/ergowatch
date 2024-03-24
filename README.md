# ErgoWatch

A data aggregation platform for the Ergo blockchain.

If looking for the frontend of https://ergo.watch see https://github.com/abchrisxyz/ergowatch-fe.

## ew

`ew` is an indexer that queries a node's API and populates the database.

### Requirements

`ew` needs a synced Ergo node and a PostgreSQL server to run with. As long as enough disk space is available, the watcher will run smoothly on most machines.

### Configuration

`ew` is configured through environment variables.

Expected:

- `EW_POSTGRES_URI`: a PostgreSQL connection string (e.g. `postgres://user:pw@host:port/db`)
- `EW_NODE_URL`: url to Ergo node (e.g. `http://node:9053`)

Optional:

- `EW_LOG`: rust's [env_logger](https://docs.rs/env_logger/latest/env_logger/)-like log level (e.g. `ew=debug`). Defaults to `ew=info`.

The `docker-compose.example.yml` might also be a good place to look at to see how things ought to be configured.

### Installation

#### Build

For a local build, install [Rust](https://www.rust-lang.org/tools/install) and then run:

```bash
cargo build --release --no-default-features
```

from within the `ew` directory. The binary will be located under `ew/target/release`.

#### Test

Unit tests can be run with `cargo test --lib`.

Integration must be run single-threaded and expect a connection to a test database: `cargo test --test '*' -- --test-threads=1`.

#### Database

`ew` will handle all schema creations and migrations. All it needs is a db connection with enough privileges.

#### Initial Sync

When running for the first time (i.e. with an empty database), the watcher will first sync core tables only, then load database constraints and populate other tables. If interrupted during the bootstrap process, it is safe to restart the watcher, it'll pick up where it left off.

### Fork handling

`ew` takes care of rolling back data when needed.

## API

The `api` directory contains a FastAPI api backend. This is a legacy api, kept operational for external services relying on it.

Docs: see https://api.ergo.watch/docs.

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


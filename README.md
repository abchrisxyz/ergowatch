# ErgoWatch
Ergo blockchain stats & monitoring.

ErgoWatch consists of a chain indexer (the "watcher") and an API exposing indexed data.

API docs available at https://ergo.watch/api/v0/docs.

If looking for the frontend of https://ergo.watch see https://github.com/abchrisxyz/ergowatch-ui.

## Roadmap

v0.1: Address counts, ERG/token balances and tokens supply/burnings

v0.2: Basic metrics (on par with old backend)

v0.3: V1 Oracle Pools (Integrating https://github.com/thedelphiproject/ergo-oracle-stats-backend)

v0.4: SigmaUSD

At this point, should be ready to retire old backend and merge with main branch.

v0.5: V2 Oracle Pools

## Watcher

An indexer that queries a node's API and populates the database.

### Requirements

As long as enough disk space is available, the watcher will run smoothly on most machines. As of now (block ~700k) the database is around 30GB.

> Note that the current implementation doesn't use streaming or any async features. Syncing from scratch with a node that is on another host will be **very slow**, even when using bootstrap mode.

### Installation

#### Build

For a local build, install [Rust](https://www.rust-lang.org/tools/install) and then run `cargo build --release` from within the `watcher` directory.

#### Test

Unit tests can be run with `cargo test`.

There is also a testbench performing a number of integration tests that need access to a Postgres server. Make sure you run `cargo build --release` before running the testbench. Refer to the README in the `testbench` directory for more details.

#### Database

The watcher expects a database with the schema defined in `watcher/db/schema.sql` and optionally with the constraints and indexes defined in  `watcher/db/constraints.sql` as well if not using `--bootstrap` mode (see initial sync further down).

If using the Dockerfiles, all of the above will be preconfigured to use bootstrap mode.

### Usage

Basic usage is like so `watcher -c <path/to/config.toml>`.

> If running from scratch or with the `--bootstrap` option, add the following option:
>
> `-k <path/to/constaints.sql`> 

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

There two strategies to perform the initial syncing of the database:

- **Normal mode**: Processes each block entirely, in sequence. Good is if disk space is an issue.
- **Bootstrap mode**: Only syncs core tables until current height is reached, then runs bootstrapping queries to fill other tables.

Bootstrap mode is roughly 3x faster than normal mode but requires at least twice the size of the database in free disk space (this is only momentarily to store PostgreSQL WAL when running the bootstrapping queries).

In bootstrap mode, the watcher will exit when done. It should be pretty close to current height when finished, but there will always be some lag due to the time taken by the bootstrapping queries (step 5).

> Bootstrap mode expects to run with an unconstrained database, so only load `schema.sql` when initializing the database. The watcher will take care of applying `constraints.sql` to the database once done. Specify the path to that sql file using the `-k` option.

Database state and command line options:

- If the database constraints have been set, watcher starts in normal mode and will not allow the `--bootstrap` flag.

- If the database constraints have not been set and the database is empty, watcher will start in bootstrap mode.

- If the database constraints have not been set and the database is *not* empty - can happen when the bootstrapping process is interrupted - then watcher will proceed in bootstrap with the `--bootstrap` flag. If omitted, watcher will not run as normal mode is not allowed with an unconstrained database.

### Indexing

The watcher only keeps main chain blocks. In the event of a chain fork, the old branch is rolled back up to the forking block and main chain blocks are included again from that point onwards.

### Processing units

When a new block is available, the watcher will query it from the node. Once obtained from the node, a block is preprocessed into a rust struct. The preprocessing step involves conversion of ergo trees into readable addresses as well as rendering of register contents into string representations. The preprocessed block then goes through a number of processing units - for lack of a better name - each responsible of extracting specific information from the block and writing it to the database. All database actions related to a block are executed within a transaction to keep all units in sync, at all times.

List of units:

- [x] **Core unit**: The first unit a block goes through, writing all core tables (headers, transactions, outputs etc.). If you're familiar with the explorer backend database you will recognise a similar schema, minus some tables and columns that aren't relevant for the statistics we're interested in. Notably, at this stage, we don't store raw ergo trees or AD proofs for instance. This helps keeping the database size to a minimum.
- [x] **Unspent unit**: Maintains a set of unspent boxes.
- [ ] **Balance unit**: Tracks address balances and balance changes for both ERG and native tokens.
- [ ] **Oracle pools** unit: Anything related to known oracle pools.
- [ ] **SigmaUSD unit**: Monitors SigmaUSD related transactions.

## API

Docs: see https://ergo.watch/api/v0/docs.

### Test

```
cd api/src/tests
pip install -r requirements.txt
pytest
```


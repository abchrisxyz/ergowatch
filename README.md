# ErgoWatch
Ergo blockchain stats & monitoring.

ErgoWatch consists of a chain indexer (the `watcher`) and an API exposing indexed data.

If looking for the frontend of https://ergo.watch see https://github.com/abchrisxyz/ergowatch-ui.

## Watcher

An indexer that queries the node and populates the database.

### Build

For a local build, install rust and then run `cargo build --release` from within the `watcher` directory.

### Test

Unit tests can be run with `cargo test`.

There is also a testbench performing a number of integration tests that need access to a Postgres server. Make sure you run `cargo build --release` before running the testbench. Refer to the README in the `testbench` directory for more details.

### Usage

To run the watcher, execute the following command `watcher -c <path/to/config.toml`.

Run `watcher -h` or `watcher --help` for more options.

### Configuration

Node url and database connection settings can be configured through a config file. See `watcher/config/default.toml` for an example.

Some config file settings can be overwritten through environment variables:

- `EW_DB_HOST`: Postgres host
- `EW_DB_PORT`: Postgres port
- `EW_DB_NAME`: Postgres name
- `EW_DB_USER`: Postgres user
- `EW_DB_PASS`: Postgres pass
- `EW_NODE_URL`: URL to Ergo node (including port, if any)

The `docker-compose.yml` might also be a good place to look at to see how things ought to be configured.

### Sync speed

The current implementation doesn't use streaming or any async features. Syncing from scratch with a node that is on another host will be very slow.

Database relations and constraints are defined in separate files to make it easy to perform the initial sync without constraints. It is recommended to initialise the database with `schema.sql` only at first and to apply `constraints.sql` when closer to current height.

Following above recommendations, a full sync (to block 682k at the time of testing) took less than 6 hours. This will likely take longer with future versions as more processing units are added.

### Indexing

The watcher only keeps main chain blocks. In the event of a fork, the side chain is rolled back up to the forking block and main-chain blocks are included from again from that point onwards.

### Processing units

When a new block is available, the watcher will query it from the node. Once obtained from the node, a block is preprocessed into a rust struct. The preprocessing involves conversion of ergo trees into readable addresses as well as rendering of register contents into string representations. The preprocessed block then goes through a number of processing units, each responsible of extracting specific information from the block and writing it to the database.

- [x] **Core unit**: The first unit a block goes through, writing all core tables (headers, transactions, outputs etc.). If you're familiar with the explorer backend database you will recognise a similar schema, minus some tables and columns that aren't relevant for the statistics we're interested in. Notably, at this stage, we don't store raw ergo trees or AD proofs for instance. This helps keeping the database size to a minimum.
- [ ] Balance unit: Syncs address balances for both ERG and native tokens.
- [ ] Oracle pool units: Anything related to known oracle pools.
- [ ] SigmaUSD unit: Monitors SigmaUSD related transactions.

And more to come.

## API

Still needs to be build. An API layer on top of the watcher's database.

### Wishlist

- [ ] address balance over time
- [ ] current address rank
- [ ] token circulating supply (accounting for token burns)


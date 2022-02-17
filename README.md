# ErgoWatch
Ergo blockchain stats & monitoring.

ErgoWatch consists of a chain indexer (the "watcher") and an API exposing indexed data.

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

#### Configuration

Node url and database connection settings can be configured through a config file. See `watcher/config/default.toml` for an example.

Some config file settings can be overwritten through environment variables:

- `EW_DB_HOST`: Postgres host
- `EW_DB_PORT`: Postgres port
- `EW_DB_NAME`: Postgres name
- `EW_DB_USER`: Postgres user
- `EW_DB_PASS`: Postgres pass
- `EW_NODE_URL`: URL to Ergo node (including port, if any)

The `docker-compose.yml` might also be a good place to look at to see how things ought to be configured.

#### Bootstrapping (wip - partly implemented)

When syncing from scratch (i.e. empty database), the watcher will start in bootstrap mode. This mode can also be invoked by passing the `-b` or `--bootstrap` option, provided database constraints haven't been set yet. Bootstrap mode does the following:

1. Check no database constraints are set
2. Skip processing of bootstrappable units until current height is reached
3. Stop syncing once current height is reached
4. Apply database constraints defined in `watcher/db/constraints.sql`
5. Run bootstrapping queries.
5. Exit.

In bootstrap mode, the watcher will exit when done. It should be pretty close to current height when finished, but there will always be some lag due to the time taken by the bootstrapping queries (step 5).

> Note that the current implementation doesn't use streaming or any async features. Syncing from scratch with a node that is on another host will be **very slow**, even when using bootstrap mode.

### Indexing

The watcher only keeps main chain blocks. In the event of a fork, the old branch is rolled back up to the forking block and main chain blocks are included again from that point onwards.

### Processing units

When a new block is available, the watcher will query it from the node. Once obtained from the node, a block is preprocessed into a rust struct. The preprocessing step involves conversion of ergo trees into readable addresses as well as rendering of register contents into string representations. The preprocessed block then goes through a number of processing units - for lack of a better name - each responsible of extracting specific information from the block and writing it to the database. All database actions related to a block are executed within a transaction to keep all units in sync, at all times.

List of units:

- [x] **Core unit**: The first unit a block goes through, writing all core tables (headers, transactions, outputs etc.). If you're familiar with the explorer backend database you will recognise a similar schema, minus some tables and columns that aren't relevant for the statistics we're interested in. Notably, at this stage, we don't store raw ergo trees or AD proofs for instance. This helps keeping the database size to a minimum.
- [ ] **Unspent unit**: Maintains a set of unspent boxes.
- [ ] **Balance unit**: Tracks address balances and balance changes for both ERG and native tokens.
- [ ] **Oracle pools** units: Anything related to known oracle pools.
- [ ] **SigmaUSD unit**: Monitors SigmaUSD related transactions.

And more to come.

## API

Still needs to be built. An API layer on top of the watcher's database.

### Wishlist

- [ ] address balance over time
- [ ] current address rank
- [ ] token circulating supply (accounting for token burns)


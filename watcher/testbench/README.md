## Running the test bench

In the `testbench` directory create a `local.py` file defining the following variables:

```python
DB_HOST = "localhost"
DB_USER = "postgres"
DB_PASS = "example"

# This is a public node, use a local one if available
NODE_URL = "http://213.239.193.208:9053"
```

The test bench uses a mocked node api. The `NODE_URL` is only used by single test, ensuring the mock api returns mimics the node correctly.

Build the watcher

```
cargo build --release
```

The run the `pytest` command from within the `testbench` directory.

## Installing Psycopg

Instructions [here](https://www.psycopg.org/psycopg3/docs/basic/install.html), but in short:

```
pip install --upgrade pip
pip3 install psycopg[binary]
```

No binary packages for Apple Silicon yet, instead do:

```
brew install libpq
pip3 install psycopg
brew link libpq --force
```


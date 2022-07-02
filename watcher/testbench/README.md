## Installation

The test bench is python 3 package, so make sure you have python 3 running on your system.

Build and install the `sigpy` package, see `sigpy/README.md` for instructions.

Install the dependencies listed in `.requirements.txt`. Psycopg can be tricky on some devices, see further instructions below.

Finally, install the test bench package itself:

```
cd testbench
python3 -m pip install -e .
```

To remove it:

```
python3 -m pip uninstall testbench
```

### Installing Psycopg

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
## Running the test bench

In the `testbench` directory create a `local.py` file defining the following variables:

```python
DB_HOST = "localhost"
DB_PORT = 5432
DB_USER = "postgres"
DB_PASS = "example"

# This is a public node, use a local one if available
NODE_URL = "http://213.239.193.208:9053"
```

The test bench uses a mocked node API. The `NODE_URL` is only used by a single test ensuring the mock API mimics the node correctly.

Build the watcher

```
cargo build --release
```

Then run the `pytest` command from within the `testbench` directory.




import re
import pytest
import psycopg as pg
import os
from pathlib import Path

from fixtures.api import MockApi, ApiUtil, GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.db import temp_db_rev1
from fixtures.db import SCHEMA_PATH
from fixtures.db import fill_rev1_db
from fixtures.addresses import AddressCatalogue as AC
from utils import run_watcher


# TODO: copied from test_core, but could be simplified a bit
def make_blocks(height: int):
    """
    Returns test blocks starting at giving height

    block a - coinbase tx:
        base-box1 1000 --> base-box2  950
                           con1-box1   50

    block b - minting a token and using registers:
        con1-box1   50 --> con2-box1   40
                           pub1-box1   10 (2000 con1-box1)

    block c using a datainput (in {}) and spending tokens:
        pub1-box1   10 --> pub1-box2    5 (1500 con1-box1)
       {con2-box1}         pub2-box1    4 ( 500 con1-box1)
                           fees-box1    1

        fees-box1    1 --> con1-box2    1
    """
    base = AC.coinbase
    fees = AC.fees
    con1 = AC.get("con1")
    con2 = AC.get("con2")
    pub1 = AC.get("pub1")
    pub2 = AC.get("pub2")

    h = height + 1
    tx_a1 = {
        "id": "tx-a1",
        "inputs": [
            {
                "boxId": "base-box1",
            }
        ],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "base-box2",
                "value": 950,
                "ergoTree": base.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-a1",
                "index": 0,
            },
            {
                "boxId": "con1-box1",
                "value": 50,
                "ergoTree": con1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-a1",
                "index": 1,
            },
        ],
        "size": 344,
    }

    h += 1
    tx_b1 = {
        "id": "tx-b1",
        "inputs": [{"boxId": "con1-box1"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "con2-box1",
                "value": 40,
                "ergoTree": con2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {
                    "R4": "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
                    "R5": "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
                    "R6": "05a4c3edd9998877",
                },
                "transactionId": "tx-b1",
                "index": 0,
            },
            {
                "boxId": "pub1-box1",
                "value": 10,
                "ergoTree": pub1.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 2000,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-b1",
                "index": 1,
            },
        ],
        "size": 674,
    }

    h += 1
    tx_c1 = {
        "id": "tx-c1",
        "inputs": [{"boxId": "pub1-box1"}],
        "dataInputs": [{"boxId": "con2-box1"}],
        "outputs": [
            {
                "boxId": "pub1-box2",
                "value": 5,
                "ergoTree": pub1.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 1500,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c1",
                "index": 0,
            },
            {
                "boxId": "pub2-box1",
                "value": 4,
                "ergoTree": pub2.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 500,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c1",
                "index": 1,
            },
            {
                "boxId": "fees-box1",
                "value": 1,
                "ergoTree": fees.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c1",
                "index": 2,
            },
        ],
        "size": 100,
    }

    tx_c2 = {
        "id": "tx-c2",
        "inputs": [
            {
                "boxId": "fees-box1",
            }
        ],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "con1-box2",
                "value": 1,
                "ergoTree": con1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c2",
                "index": 0,
            }
        ],
        "size": 100,
    }

    block_a = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560100000,
            "size": 123,
            "height": height + 1,
            "id": "block-a",
            "parentId": GENESIS_ID,
        },
        "blockTransactions": {
            "headerId": "block-a",
            "transactions": [tx_a1],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    block_b = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560200000,
            "size": 123,
            "height": height + 2,
            "id": "block-b",
            "parentId": "block-a",
        },
        "blockTransactions": {
            "headerId": "block-b",
            "transactions": [tx_b1],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    block_c = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560200000,
            "size": 123,
            "height": height + 3,
            "id": "block-c",
            "parentId": "block-b",
        },
        "blockTransactions": {
            "headerId": "block-c",
            "transactions": [tx_c1, tx_c2],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    return [block_a, block_b, block_c]


def get_number_of_migrations() -> int:
    """
    Returns number of files in migrations directory
    """
    p = Path(__file__).parent / Path("../src/db/migrations")
    return len(os.listdir(p.resolve()))


@pytest.mark.order(5)
class TestMigrations:
    start_height = 100_000

    @pytest.fixture(scope="class")
    def api(self):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(blocks)
            yield api

    @pytest.fixture()
    def db(self, api, temp_db_rev1):
        """
        Loads mock api and provides test db connection.

        Function scoped fixture since tests will be changing the db state.
        """
        blocks = make_blocks(self.start_height)
        with pg.connect(temp_db_rev1) as conn:
            # Fill genesis and 1st block only, so we can check if any new blocks got included
            fill_rev1_db(conn, blocks[0:1])
            yield conn

    def test_future_db(self, db: pg.Connection, temp_cfg):
        """
        Check watcher stops if DB was created by future version.
        """
        with db.cursor() as cur:
            # Check state before run
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 2
            # Fake migration
            cur.execute("update ew.revision set version = version + 1000;")
        db.commit()

        cp = run_watcher(temp_cfg)

        # Check logs
        assert (
            "Database was created by a more recent version of this program"
            in cp.stdout.decode()
        )

        # Check nothing happened
        assert cp.returncode != 0
        with db.cursor() as cur:
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 2

    def test_lagging_db_without_m_option(self, db: pg.Connection, temp_cfg):
        """
        Check watcher stops if DB is behind but migrations are not allowed.
        """
        # Get revision of latest schema
        with open(SCHEMA_PATH) as f:
            current_revision = re.findall(
                r"insert into ew\.revision \(version\) values \((\d+)\)", f.read()
            )[0]
            f.read()
            current_revision = int(current_revision)

        # Check schema revision matches number of migrations.
        # Migration 1 results in revision 2, and so on...
        assert current_revision == get_number_of_migrations() + 1

        with db.cursor() as cur:
            # Check state before run
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 2
            # Obtain current db revision
            cur.execute("select version from ew.revision;")
            db_revision = cur.fetchone()[0]
            assert db_revision < current_revision
            assert db_revision == 1

        cp = run_watcher(temp_cfg)

        # Check logs
        assert (
            f"Database is {current_revision - db_revision} revision(s) behind. Run with the -m option to allow migrations to be applied."
            in cp.stdout.decode()
        )

        # Check nothing happened
        assert cp.returncode != 0
        with db.cursor() as cur:
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 2

    def test_migrations_are_applied_if_allowed(self, db: pg.Connection, temp_cfg):
        """
        Check migrations are applied and watcher proceeds normally.
        """
        # Check db is at version 1 initially
        with db.cursor() as cur:
            cur.execute("select version from ew.revision;")
            assert cur.fetchone()[0] == 1

        cp = run_watcher(temp_cfg, allow_migrations=True)

        # Check logs
        assert cp.returncode == 0
        n_migs = get_number_of_migrations()
        rev = n_migs + 1
        assert f"Applying migration {n_migs} (revision {rev})" in cp.stdout.decode()

        # Check migrations are applied
        with db.cursor() as cur:
            cur.execute("select version from ew.revision;")
            assert cur.fetchone()[0] == rev

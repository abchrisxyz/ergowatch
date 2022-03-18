import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil, GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.db2 import bootstrap_db
from fixtures.db2 import temp_db_class_scoped
from fixtures.db2 import unconstrained_db_class_scoped
from fixtures.addresses import AddressCatalogue as AC
from utils import run_watcher
from utils import assert_pk
from utils import assert_index
from utils import assert_column_ge


ORDER = 12


def make_blocks(height: int):
    """
    Returns test blocks starting at giving height

    block a - coinbase tx:
        base-box1 1000 --> base-box2  950
                           con1-box1   50

    ----------------------fork-of-b----------------------
    block x - fork of block b to be ignored/rolled back:
        con1-box1   50 --> con9-box1   30
                           pub9-box1   20 (3000 con1-box1)
    ------------------------------------------------------

    block b - minting a token and using registers:
        con1-box1   50 --> con2-box1   40
                           pub1-box1   10 (2000 con1-box1)

    block c using a datainput (in {}) and spending tokens:
        pub1-box1   10 --> pub1-box2    6 (1500 con1-box1)
       {con2-box1}         pub2-box1    3 ( 500 con1-box1)
                           fees-box1    1

        # intra-block box spend
        fees-box1    1 --> con1-box2    1

        # intra-block partial spend
        pub2-box1    3 --> pub2-box2    2 ( 400 con1-box1)
        pub1-box2    6     pub1-box3    7 (1600 con1-box1)

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
    tx_x1 = {
        "id": "tx-x1",
        "inputs": [{"boxId": "con1-box1"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "con9-box1",
                "value": 30,
                "ergoTree": con2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {
                    "R4": "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8",
                    "R5": "0e2098479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",
                    "R6": "05a4c3edd9998877",
                },
                "transactionId": "tx-x1",
                "index": 0,
            },
            {
                "boxId": "pub9-box1",
                "value": 20,
                "ergoTree": pub1.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 3000,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-x1",
                "index": 1,
            },
        ],
        "size": 674,
    }

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
                "value": 6,
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
                "value": 3,
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

    tx_c3 = {
        "id": "tx-c3",
        "inputs": [{"boxId": "pub2-box1"}, {"boxId": "pub1-box2"}],
        "dataInputs": [{"boxId": "con2-box1"}],
        "outputs": [
            {
                "boxId": "pub2-box2",
                "value": 2,
                "ergoTree": pub2.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 400,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c3",
                "index": 0,
            },
            {
                "boxId": "pub1-box3",
                "value": 7,
                "ergoTree": pub1.ergo_tree,
                "assets": [
                    {
                        "tokenId": "con1-box1",
                        "amount": 1600,
                    }
                ],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c3",
                "index": 1,
            },
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

    block_x = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560200000,
            "size": 123,
            "height": height + 2,
            "id": "block-x",
            "parentId": "block-a",
        },
        "blockTransactions": {
            "headerId": "block-x",
            "transactions": [tx_x1],
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
            "transactions": [tx_c1, tx_c2, tx_c3],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    return [block_a, block_x, block_b, block_c]


@pytest.mark.order(ORDER)
class TestSync:
    """
    Start with bootstrapped db.
    """

    start_height = 599_999

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, blocks)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-c" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


@pytest.mark.order(ORDER)
class TestSyncRollback:
    """
    Start with bootstrapped db.
    Forking scenario triggering a rollback.
    """

    start_height = 599_999

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()

            # Initially have blocks a and x only
            first_blocks = blocks[0:2]
            api.set_blocks(first_blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, first_blocks)

            # Run to include block x
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-x" in cp.stdout.decode()

            # Now make all blocks visible
            api.set_blocks(blocks)

            # Run again
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Rolling back block block-x" in cp.stdout.decode()
            assert "Including block block-b" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


@pytest.mark.order(ORDER)
class TestSyncNoForkChild:
    """
    Start with bootstrapped db.
    Scenario where node has two block candidates for last height.
    """

    start_height = 599_999

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()

            # Initially have blocks a, b and x
            first_blocks = blocks[0:3]
            api.set_blocks(first_blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, first_blocks)

            # 1 st run
            # No way to tell fork appart, should pick 1st block in alphabetical order (block-b)
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-b" in cp.stdout.decode()
            assert "Including block block-x" not in cp.stdout.decode()
            assert "no child" not in cp.stdout.decode()

            # Now make all blocks visible
            api.set_blocks(blocks)

            # Run again
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-c" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


@pytest.mark.order(ORDER)
class TestGenesis:
    """
    Start with empty, unconstrained db.
    """

    start_height = 0

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, unconstrained_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(blocks)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Bootstrapping step 1/2 - syncing core tables" in cp.stdout.decode()

            with pg.connect(unconstrained_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


@pytest.mark.order(ORDER)
class TestGenesisNoBootstrap:
    """
    Start with empty, unconstrained db and --no-bootstrap flag.
    """

    start_height = 0

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, unconstrained_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(blocks)

            # Run
            cp = run_watcher(temp_cfg, no_bootstrap=True)
            assert cp.returncode == 0
            assert "Found --no-bootstrap flag" in cp.stdout.decode()
            assert "Synchronizing with node" in cp.stdout.decode()

            with pg.connect(unconstrained_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


def _test_db_state(conn: pg.Connection, start_height: int):
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_erg_balances(cur)
        assert_erg_diffs(cur, start_height)


def assert_db_constraints(conn: pg.Connection):
    # Erg bal
    assert_pk(conn, "bal", "erg", ["address"])
    assert_column_ge(conn, "bal", "erg", "value", 0)
    assert_index(conn, "bal", "erg", "erg_value_idx")

    # Erg diffs
    assert_pk(conn, "bal", "erg_diffs", ["address", "height", "tx_id"])
    assert_index(conn, "bal", "erg_diffs", "erg_diffs_height_idx")


def assert_erg_balances(cur: pg.Cursor):
    base = AC.coinbase
    fees = AC.fees
    con1 = AC.get("con1")
    con2 = AC.get("con2")
    pub1 = AC.get("pub1")
    pub2 = AC.get("pub2")
    cur.execute("select address, value from bal.erg order by 1;")
    rows = cur.fetchall()
    assert len(rows) == 5
    assert rows[0] == (base.address, 950)
    assert rows[1] == (con1.address, 1)
    assert rows[2] == (con2.address, 40)
    assert rows[3] == (pub1.address, 7)
    assert rows[4] == (pub2.address, 2)


def assert_erg_diffs(cur: pg.Cursor, start_height: int):
    base = AC.coinbase
    fees = AC.fees
    con1 = AC.get("con1")
    con2 = AC.get("con2")
    pub1 = AC.get("pub1")
    pub2 = AC.get("pub2")
    h = start_height
    cur.execute(
        "select height, tx_id, address, value from bal.erg_diffs order by 1, 2, 3;"
    )
    rows = cur.fetchall()
    assert len(rows) == 13

    bootstrap_tx_id = GENESIS_ID if start_height == 0 else "bootstrap-tx"
    assert rows[0] == (h + 0, bootstrap_tx_id, base.address, 1000)

    assert rows[1] == (h + 1, "tx-a1", base.address, -50)
    assert rows[2] == (h + 1, "tx-a1", con1.address, 50)

    assert rows[3] == (h + 2, "tx-b1", con1.address, -50)
    assert rows[4] == (h + 2, "tx-b1", con2.address, 40)
    assert rows[5] == (h + 2, "tx-b1", pub1.address, 10)

    assert rows[6] == (h + 3, "tx-c1", fees.address, 1)
    assert rows[7] == (h + 3, "tx-c1", pub1.address, -4)
    assert rows[8] == (h + 3, "tx-c1", pub2.address, 3)

    assert rows[9] == (h + 3, "tx-c2", fees.address, -1)
    assert rows[10] == (h + 3, "tx-c2", con1.address, 1)

    assert rows[11] == (h + 3, "tx-c3", pub1.address, 1)
    assert rows[12] == (h + 3, "tx-c3", pub2.address, -1)

import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil, GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.db import bootstrap_db
from fixtures.db import fill_rev1_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import temp_db_rev1_class_scoped
from fixtures.db import unconstrained_db_class_scoped
from fixtures.addresses import AddressCatalogue as AC
from utils import run_watcher
from utils import assert_pk
from utils import assert_fk
from utils import assert_unique
from utils import assert_column_not_null
from utils import assert_index

ORDER = 13


def make_blocks(height: int):
    """
    Returns test blocks starting at giving height.

    block a:
        -- coinbase tx:
        base-box1 1000 --> base-box2  950
                           con1-box1   50

    block b:
        -- deposit 10 to CEX 1:
        con1-box1   50 --> pub1-box1   10
                           con1-box2   40

    block c:
        -- deposit 15 to CEX 2
        con1-box2   40 --> pub2-box1   15
                           con1-box3   25

        -- deposit 5 to CEX 3
        con1-box3   25 --> pub3-box1   20
                           con1-box4    5

        -- cex 1 claiming deposit (deposit was sold)
        pub1-box1   10 --> cex1-box1   10

    ----------------------fork-of-d----------------------
    block x - fork of block d to be ignored/rolled back:
        -- cex 3 claiming deposit (deposit was sold)
        pub3-box1   20 --> cex3-box1   20
    ------------------------------------------------------

    block d:
        -- cex 2 claiming part of deposit (some deposit was sold)
        pub2-box1   15 --> cex2-box1    5
                           pub2-box2    9
                           fees-box1    1

    block e - one more block to tell d and x appart
        -- dummy tx
        fees-box1    1 --> fees-box2  1

    """
    base = AC.coinbase
    fees = AC.fees
    con1 = AC.get("con1")
    pub1 = AC.get("pub1")
    pub2 = AC.get("pub2")
    pub3 = AC.get("pub3")
    cex1 = AC.get("cex1")
    cex2 = AC.get("cex2")
    cex3 = AC.get("cex3")

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
                "boxId": "pub1-box1",
                "value": 10,
                "ergoTree": pub1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-b1",
                "index": 0,
            },
            {
                "boxId": "con1-box2",
                "value": 40,
                "ergoTree": con1.ergo_tree,
                "assets": [],
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
        "inputs": [{"boxId": "con1-box2"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "pub2-box1",
                "value": 15,
                "ergoTree": pub2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c1",
                "index": 0,
            },
            {
                "boxId": "con1-box3",
                "value": 25,
                "ergoTree": con1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c1",
                "index": 1,
            },
        ],
        "size": 100,
    }

    tx_c2 = {
        "id": "tx-c2",
        "inputs": [{"boxId": "con1-box3"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "pub3-box1",
                "value": 20,
                "ergoTree": pub3.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c2",
                "index": 0,
            },
            {
                "boxId": "con1-box4",
                "value": 5,
                "ergoTree": con1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c2",
                "index": 1,
            },
        ],
        "size": 100,
    }

    tx_c3 = {
        "id": "tx-c3",
        "inputs": [{"boxId": "pub1-box1"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "cex1-box1",
                "value": 10,
                "ergoTree": cex1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-c3",
                "index": 0,
            },
        ],
        "size": 100,
    }

    h += 1
    tx_x1 = {
        "id": "tx-x1",
        "inputs": [{"boxId": "pub3-box1"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "cex3-box1",
                "value": 20,
                "ergoTree": cex3.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-x1",
                "index": 0,
            },
        ],
        "size": 674,
    }

    tx_d1 = {
        "id": "tx-d1",
        "inputs": [{"boxId": "pub2-box1"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "cex2-box1",
                "value": 5,
                "ergoTree": cex2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-d1",
                "index": 0,
            },
            {
                "boxId": "pub2-box2",
                "value": 9,
                "ergoTree": pub2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-d1",
                "index": 0,
            },
            {
                "boxId": "fees-box1",
                "value": 1,
                "ergoTree": fees.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-d1",
                "index": 1,
            },
        ],
        "size": 100,
    }

    h = +1
    tx_e1 = {
        "id": "tx-e1",
        "inputs": [{"boxId": "fees-box1"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "fees-box2",
                "value": 1,
                "ergoTree": fees.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-e1",
                "index": 0,
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
            "timestamp": 1234560300000,
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

    block_x = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560400000,
            "size": 123,
            "height": height + 4,
            "id": "block-x",
            "parentId": "block-c",
        },
        "blockTransactions": {
            "headerId": "block-x",
            "transactions": [tx_x1],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    block_d = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560400000,
            "size": 123,
            "height": height + 4,
            "id": "block-d",
            "parentId": "block-c",
        },
        "blockTransactions": {
            "headerId": "block-d",
            "transactions": [tx_d1],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    block_e = {
        "header": {
            "votes": "000000",
            "timestamp": 1234560500000,
            "size": 123,
            "height": height + 5,
            "id": "block-e",
            "parentId": "block-d",
        },
        "blockTransactions": {
            "headerId": "block-e",
            "transactions": [tx_e1],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    return [block_a, block_b, block_c, block_x, block_d, block_e]


@pytest.mark.skip("Not implemented")
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


@pytest.mark.skip("Not implemented")
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


@pytest.mark.skip("Not implemented")
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


@pytest.mark.skip("Not implemented")
@pytest.mark.order(ORDER)
class TestMigrations:
    """
    Aplly migration to synced db
    """

    start_height = 599_999

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_rev1_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        blocks = make_blocks(self.start_height)
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(blocks)

            # Prepare db
            with pg.connect(temp_db_rev1_class_scoped) as conn:
                fill_rev1_db(conn, blocks)

            # Run
            cp = run_watcher(temp_cfg, allow_migrations=True)
            assert cp.returncode == 0
            assert "Applying migration 1 (revision 2)" in cp.stdout.decode()

            with pg.connect(temp_db_rev1_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height)


def _test_db_state(conn: pg.Connection, start_height: int):
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_supply_details(cur, start_height)
        assert_supply(cur, start_height)


def assert_db_constraints(conn: pg.Connection):
    # mtr.cex_supply_details
    assert_pk(conn, "mtr", "cex_supply_details", ["height", "cex_id"])
    assert_fk(conn, "mtr", "cex_supply_details", "cex_supply_details_cex_id_fkey")
    # mtr.cex_supply
    assert_pk(conn, "mtr", "cex_supply", ["address"])


def assert_supply_details(cur: pg.Cursor, start_height: int):
    height_c = start_height + 3
    height_d = start_height + 4
    cur.execute(
        """
        select height
            , cex_id
            , main
            , deposit
        from mtr.cex_supply_details
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    for row in rows:
        print(row)
    assert len(rows) == 2
    # At b, pub1 not linked to cex 1 yet
    # At c, pub2 not linked to cex 2 yet
    assert rows[0] == (height_c, 1, 10, 0)
    assert rows[1] == (height_d, 2, 5, 9)


def assert_supply(cur: pg.Cursor, start_height: int):
    cur.execute(
        """
        select height
            , total
            , deposit
        from mtr.cex_supply
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    for row in rows:
        print(row)
    assert len(rows) == 5
    assert rows[0] == (start_height + 0, 0, 0)
    assert rows[1] == (start_height + 1, 0, 0)
    assert rows[2] == (start_height + 2, 0, 0)
    assert rows[3] == (start_height + 3, 10, 0)
    assert rows[4] == (start_height + 4, 15, 9)
    assert rows[5] == (start_height + 5, 15, 9)

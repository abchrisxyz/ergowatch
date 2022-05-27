import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil, GENESIS_ID
from fixtures.config import temp_cfg
from fixtures import syntax
from fixtures.db import bootstrap_db
from fixtures.db import fill_rev1_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import temp_db_rev1_class_scoped
from fixtures.db import unconstrained_db_class_scoped
from fixtures.addresses import AddressCatalogue as AC
from utils import run_watcher
from utils import assert_pk
from utils import assert_column_not_null
from utils import assert_column_ge


ORDER = 13


def make_blocks2(height: int):
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


def make_blocks(parent_height: int):
    """Returns test blocks starting at next height."""

    desc = """
    // pub1 is a deposit address for cex1
    // pub2 is a deposit address for cex2
    // pub3 is a deposit address for cex3
    //
    // pub9 appears as a deposit address for cex1 at first
    // but later sends to cex3 too.
    block-a
        // coinbase tx:
        base-box1 1000
        >
        base-box2  840
        con1-box1   60
        pub9-box1  100

    block-b
        // deposit 20 to CEX 1:
        con1-box1   60
        >
        pub1-box1   10
        pub1-box2   10
        con1-box2   40
        --
        // false positive
        // pub9 will be linked to more than 1 cex
        pub9-box1  100
        >
        cex1-box1    6
        pub9-box2   94

    block-c
        // deposit 15 to CEX 2
        con1-box2   40
        >
        pub2-box1   15
        con1-box3   25
        --
        // deposit 5 to CEX 3 (hidden)
        con1-box3   25
        >
        pub3-box1   20
        con1-box4    5
        --
        // cex 1 claiming deposit (deposit was sold)
        pub1-box1   10
        >
        cex1-box2   10

    // ----------------------fork-of-d----------------------
    block-x // fork of block d to be ignored/rolled back:
        -// cex 3 claiming deposit (deposit was sold)
        pub3-box1   20
        >
        cex3-box1   20
        --
        // fake false positive
        // would link pub1 to cex 2 as well
        // to test a conflict rollback
        pub1-box2   10
        >
        cex2-box1   10
    //------------------------------------------------------

    block-d-c
        // cex 2 claiming part of deposit (some deposit was sold)
        pub2-box1   15
        >
        cex2-box1    5
        pub2-box2    9
        fees-box1    1

    //one more block to tell d and x appart and test known deposit addresses
    block-e
        // new cex 2 claim (to test same address is added only once)
        pub2-box2    9
        >
        cex2-box2   3
        pub2-box3   6
        --
        // false positive for deposit addres
        // now linked to a second cex
        // erg still ends up on main though
        pub9-box2   94
        >
        cex3-box2   94
        --
        // contract tx to be ignored
        // con1 will be ignored as deposit address'
        // but supply on cex3 main will increase
        con1-box4    5
        >
        cex3-box3    5
    """
    return syntax.parse(desc, parent_height + 1)


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

            # Initially have chain a-b-c-x
            first_blocks = blocks[0:4]
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
            assert "Including block block-d" in cp.stdout.decode()

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
        _test_db_state(synced_db, self.start_height, bootstrapped=True)


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
        _test_db_state(synced_db, self.start_height, bootstrapped=True)


@pytest.mark.order(ORDER)
class TestRepair:
    """
    Same as TestSync, but triggering a repair event after full sync.
    """

    # Start one block later so last block has height multiple of 5
    # and trigger a repair event.
    start_height = 599_999 + 1

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
                # Simulate an interupted repair,
                # Should be cleaned up at startup.
                with conn.cursor() as cur:
                    cur.execute("create schema repair;")
                conn.commit()

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-e" in cp.stdout.decode()
            assert "Repairing 4 blocks (600002 to 600005)" in cp.stdout.decode()
            assert "Done repairing heights 600002 to 600005" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height, bootstrapped=True)


def _test_db_state(conn: pg.Connection, start_height: int, bootstrapped=False):
    """
    Test outcomes can be different for cases that trigger bootstrapping code or
    a repair event. This is indicated through the *bootstrapped* flag.

    TestSync and SyncRollback trigger no bootstrap and no repair.
    TestGenesis and TestMigrations will bootstrap their cex schema.
    TestRepair does no bootstrap but ends with a repair and so produces
    the same state as TestGeneis and TestMigrations.
    """
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_supply(cur, start_height, bootstrapped)


def assert_db_constraints(conn: pg.Connection):
    # mtr.cex_supply
    assert_pk(conn, "mtr", "cex_supply", ["height"])
    assert_column_not_null(conn, "mtr", "cex_supply", "height")
    assert_column_not_null(conn, "mtr", "cex_supply", "total")
    assert_column_not_null(conn, "mtr", "cex_supply", "deposit")
    assert_column_ge(conn, "mtr", "cex_supply", "total", 0)
    assert_column_ge(conn, "mtr", "cex_supply", "deposit", 0)


def assert_supply(cur: pg.Cursor, start_height: int, bootstrapped: bool):
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
    if bootstrapped:
        assert len(rows) == 6
        assert rows[0] == (start_height + 0, 0, 0)
        assert rows[1] == (start_height + 1, 0, 0)
        assert rows[2] == (start_height + 2, 26, 20)
        assert rows[3] == (start_height + 3, 41, 25)
        assert rows[4] == (start_height + 4, 40, 19)
        assert rows[5] == (start_height + 5, 139, 16)
    else:
        assert len(rows) == 6
        assert rows[0] == (start_height + 0, 0, 0)
        assert rows[1] == (start_height + 1, 0, 0)
        assert rows[2] == (start_height + 2, 100, 94)
        assert rows[3] == (start_height + 3, 120, 104)
        assert rows[4] == (start_height + 4, 134, 113)
        assert rows[5] == (start_height + 5, 233, 110)

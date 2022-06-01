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
from utils import assert_column_ge

ORDER = 13


def make_blocks(height: int):
    """
    Returns test blocks starting at giving height.

    block a:
        -- coinbase tx:
        base-box1 1000 --> base-box2  840
                           con1-box1   60
                           pub9-box1  100

    block b:
        -- deposit 10 to CEX 1:
        con1-box1   60 --> pub1-box1   10
                           pub1-box2   10
                           con1-box2   40

        -- false positive
        -- pub9 will be linked to more than 1 cex
        pub9-box1  100 --> cex1-box1   10
                           pub9-box2   90

    block c:
        -- deposit 15 to CEX 2
        con1-box2   40 --> pub2-box1   15
                           con1-box3   25

        -- deposit 5 to CEX 3
        con1-box3   25 --> pub3-box1   20
                           con1-box4    5

        -- cex 1 claiming deposit (deposit was sold)
        pub1-box1   10 --> cex1-box2   10

    ----------------------fork-of-d----------------------
    block x - fork of block d to be ignored/rolled back:
        -- cex 3 claiming deposit (deposit was sold)
        pub3-box1   20 --> cex3-box1   20

        -- fake false positive
        -- would link pub1 to cex 2 as well
        -- to test a conflict rollback
        pub1-box2   10 --> cex2-box1   10
    ------------------------------------------------------

    block d:
        -- cex 2 claiming part of deposit (some deposit was sold)
        pub2-box1   15 --> cex2-box1    5
                           pub2-box2    9
                           fees-box1    1

    block e
    one more block to tell d and x appart and test known deposit addresses
        -- new cex 2 claim (to test same address is added only once)
        pub2-box2    9 --> cex2-box2   3
                           pub2-box3   6

        -- false positive
        -- now linked to a second cex
        pub9-box2   90 --> cex3-box2   90

        -- contract tx to be ignored
        con1-box4    5 --> cex3-box3    5


    """
    base = AC.coinbase
    fees = AC.fees
    con1 = AC.get("con1")
    pub1 = AC.get("pub1")
    pub2 = AC.get("pub2")
    pub3 = AC.get("pub3")
    pub9 = AC.get("pub9")
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
                "value": 840,
                "ergoTree": base.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-a1",
                "index": 0,
            },
            {
                "boxId": "con1-box1",
                "value": 60,
                "ergoTree": con1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-a1",
                "index": 1,
            },
            {
                "boxId": "pub9-box1",
                "value": 100,
                "ergoTree": pub9.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-a1",
                "index": 2,
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
                "boxId": "pub1-box2",
                "value": 10,
                "ergoTree": pub1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-b1",
                "index": 1,
            },
            {
                "boxId": "con1-box2",
                "value": 40,
                "ergoTree": con1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-b1",
                "index": 2,
            },
        ],
        "size": 674,
    }

    tx_b2 = {
        "id": "tx-b2",
        "inputs": [{"boxId": "pub9-box1"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "cex1-box1",
                "value": 10,
                "ergoTree": cex1.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-b2",
                "index": 0,
            },
            {
                "boxId": "pub9-box2",
                "value": 90,
                "ergoTree": pub9.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-b2",
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
                "boxId": "cex1-box2",
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

    tx_x2 = {
        "id": "tx-x2",
        "inputs": [{"boxId": "pub1-box2"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "cex2-box1",
                "value": 10,
                "ergoTree": cex2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-x2",
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
        "inputs": [{"boxId": "pub2-box2"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "cex2-box2",
                "value": 3,
                "ergoTree": cex2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-e1",
                "index": 0,
            },
            {
                "boxId": "pub2-box3",
                "value": 6,
                "ergoTree": pub2.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-e1",
                "index": 1,
            },
        ],
        "size": 100,
    }

    tx_e2 = {
        "id": "tx-e2",
        "inputs": [{"boxId": "pub9-box2"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "cex3-box2",
                "value": 90,
                "ergoTree": cex3.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-e2",
                "index": 0,
            },
        ],
        "size": 100,
    }

    tx_e3 = {
        "id": "tx-e3",
        "inputs": [{"boxId": "con1-box4"}],
        "dataInputs": [],
        "outputs": [
            {
                "boxId": "cex3-box3",
                "value": 5,
                "ergoTree": cex3.ergo_tree,
                "assets": [],
                "creationHeight": h,
                "additionalRegisters": {},
                "transactionId": "tx-e3",
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
            "transactions": [tx_b1, tx_b2],
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
            "transactions": [tx_x1, tx_x2],
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
            "transactions": [tx_e1, tx_e2, tx_e3],
            "blockVersion": 2,
            "size": 1155,
        },
        "size": 1000,
    }

    return [block_a, block_b, block_c, block_x, block_d, block_e]


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

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-c" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.start_height, bootstrapped=True)


def _test_db_state(conn: pg.Connection, start_height: int, bootstrapped=False):
    """
    Test outcomes can be different for cases that trigger bootstrapping code.
    This is indicated through the *bootstrapped* flag.
    """
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_cex_ids(cur)
        assert_main_addresses(cur)
        assert_deposit_addresses(cur)
        assert_addresses_conflicts(cur, start_height)
        assert_processing_log(cur, start_height, bootstrapped)


def assert_db_constraints(conn: pg.Connection):
    # cex.cexs
    assert_pk(conn, "cex", "cexs", ["id"])
    assert_unique(conn, "cex", "cexs", ["name"])
    # cex.addresses
    assert_pk(conn, "cex", "addresses", ["address"])
    assert_fk(conn, "cex", "addresses", "addresses_cex_id_fkey")
    assert_column_not_null(conn, "cex", "addresses", "type")
    assert_index(conn, "cex", "addresses", "addresses_cex_id_idx")
    assert_index(conn, "cex", "addresses", "addresses_type_idx")
    assert_index(conn, "cex", "addresses", "addresses_spot_height_idx")
    # cex.addresses_conflicts
    assert_pk(conn, "cex", "addresses_conflicts", ["address"])
    assert_fk(
        conn, "cex", "addresses_conflicts", "addresses_conflicts_first_cex_id_fkey"
    )
    # cex.block_processing_log
    assert_pk(conn, "cex", "block_processing_log", ["header_id"])
    assert_index(conn, "cex", "block_processing_log", "block_processing_log_status_idx")
    # cex.supply
    assert_pk(conn, "cex", "supply", ["height", "cex_id"])
    assert_fk(conn, "cex", "supply", "supply_cex_id_fkey")
    assert_index(conn, "cex", "supply", "supply_height_idx")
    assert_column_ge(conn, "cex", "supply", "main", 0)
    assert_column_ge(conn, "cex", "supply", "deposit", 0)


def assert_cex_ids(cur: pg.Cursor):
    cur.execute(
        """
        select id
            , name
        from cex.cexs
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 4
    assert rows == [
        (1, "Coinex"),
        (2, "Gate.io"),
        (3, "KuCoin"),
        (4, "ProBit"),
    ]


def assert_main_addresses(cur: pg.Cursor):
    cur.execute(
        """
        select cex_id
            , address
        from cex.addresses
        where type = 'main'
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 16
    assert rows == [
        # Coinex
        (1, "9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU"),
        (1, "9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe"),
        # Gate
        (2, "9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE"),
        (2, "9exS2B892HTiDkqhcWnj1nzsbYmVn7ameVb1d2jagUWTqaLxfTX"),
        (2, "9fJzuyVaRLM9Q3RZVzkau1GJVP9TDiW8GRL5p25VZ8VNXurDpaw"),
        (2, "9gck4LwHJK3XV2wXdYdN5S9Fe4RcFrkaqs4WU5aeiKuodJyW7qq"),
        (2, "9gmb745thQTyoGGWxSr9hNmvipivgVbQGA6EJnBucs3nwi9yqoc"),
        (2, "9gv4qw7RtQyt3khtnQNxp7r7yuUazWWyTGfo7duqGj9hMtZxKP1"),
        (2, "9i1ETULiCnGMtppDAvrcYujhxX18km3ge9ZEDMnZPN6LFQbttRF"),
        (2, "9i7134eY3zUotQyS8nBeZDJ3SWbTPn117nCJYi977FBn9AaxhZY"),
        (2, "9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC"),
        #  KuCoin
        (3, "9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr"),
        (3, "9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu"),
        (3, "9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3"),
        (3, "9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT"),
        # Probit
        (4, "9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB"),
    ]


def assert_deposit_addresses(cur: pg.Cursor):
    pub1 = AC.get("pub1")
    pub2 = AC.get("pub2")
    cur.execute(
        """
        select cex_id
            , address
        from cex.addresses
        where type = 'deposit'
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 2
    assert rows == [
        (1, pub1.address),
        (2, pub2.address),
    ]


def assert_addresses_conflicts(cur: pg.Cursor, start_height):
    pub9 = AC.get("pub9")
    cur.execute(
        """
        select address
            , first_cex_id
            , type
            , spot_height
            , conflict_spot_height
        from cex.addresses_conflicts
        order by spot_height;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows == [
        (pub9.address, 1, "deposit", start_height + 2, start_height + 5),
    ]


def assert_processing_log(cur: pg.Cursor, start_height: int, bootstrapped):
    cur.execute(
        """
        select header_id
            , height
            , invalidation_height
            , status
        from cex.block_processing_log
        order by height;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 6
    expected_status = "processed" if bootstrapped else "pending"
    assert rows[0] == (GENESIS_ID, start_height + 0, None, "processed")
    assert rows[1] == ("block-a", start_height + 1, None, expected_status)
    assert rows[2] == ("block-b", start_height + 2, None, expected_status)
    assert rows[3] == ("block-c", start_height + 3, start_height + 2, expected_status)
    assert rows[4] == ("block-d", start_height + 4, start_height + 3, expected_status)
    assert rows[5] == ("block-e", start_height + 5, None, expected_status)


def assert_supply(cur: pg.Cursor, start_height: int):
    height_b = start_height + 2
    height_c = start_height + 3
    height_d = start_height + 4
    height_e = start_height + 5
    cur.execute(
        """
        select height
            , cex_id
            , main
            , deposit
        from cex.supply
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows == [
        (height_b, 1, 0, 10),
        (height_c, 2, 0, 15),
        (height_c, 3, 0, 5),
        (height_c, 1, 10, 0),
        (height_d, 2, 5, 9),
        (height_e, 2, 8, 6),
    ]

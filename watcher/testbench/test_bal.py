import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil
from fixtures.scenario import Scenario
from fixtures.scenario.genesis import GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.db import bootstrap_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import unconstrained_db_class_scoped

from test_mtr_cex import SCENARIO_DESCRIPTION
from utils import run_watcher
from utils import assert_pk
from utils import assert_index
from utils import assert_column_ge
from utils import assert_column_not_null


ORDER = 12

SCENARIO_DESCRIPTION = """
    block-a
        base-box1 1000
        >
        base-box2  950
        con1-box1   50

    //----------------------fork-of-b----------------------
    block-x - fork of block b to be ignored/rolled back:
        con1-box1   50
        >
        con9-box1   30
        pub9-box1   20 (con1-box1: 3000)
    //------------------------------------------------------

    block-b-a
        con1-box1   50
        >
        con2-box1   40
        pub1-box1   10 (con1-box1: 2000)

    block-c
        pub1-box1   10
        {con2-box1}
        >
        pub1-box2    6 (con1-box1: 1500, pub1-box1: 50)
        pub2-box1    3 (con1-box1: 500)
        fees-box1    1
        --
        fees-box1    1
        >
        con1-box2    1
        --
        // intra-block partial spend of token con1-box1
        // full burning of token pub1-box1
        pub2-box1    3
        pub1-box2    6
        >
        pub2-box2    2 (con1-box1: 400)
        pub1-box3    7 (con1-box1: 1600)
    """


@pytest.mark.order(ORDER)
class TestSync:
    """
    Start with bootstrapped db.
    """

    scenario = Scenario(SCENARIO_DESCRIPTION, 599_999, 1234560000000)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, self.scenario)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-c" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


@pytest.mark.order(ORDER)
class TestSyncRollback:
    """
    Start with bootstrapped db.
    Forking scenario triggering a rollback.
    """

    scenario = Scenario(SCENARIO_DESCRIPTION, 599_999, 1234560000000)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi() as api:
            api = ApiUtil()

            # Initially have blocks a and x only
            self.scenario.mask(2)
            api.set_blocks(self.scenario.blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, self.scenario)

            # Run to include block x
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-x" in cp.stdout.decode()

            # Now make all blocks visible
            self.scenario.unmask()
            api.set_blocks(self.scenario.blocks)

            # Run again
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Rolling back block block-x" in cp.stdout.decode()
            assert "Including block block-b" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


@pytest.mark.order(ORDER)
class TestSyncNoForkChild:
    """
    Start with bootstrapped db.
    Scenario where node has two block candidates for last height.
    """

    scenario = Scenario(SCENARIO_DESCRIPTION, 599_999, 1234560000000)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi() as api:
            api = ApiUtil()

            # Initially have blocks a, b and x
            self.scenario.mask(3)
            api.set_blocks(self.scenario.blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, self.scenario)

            # 1 st run
            # No way to tell fork appart, should pick 1st block in appearance order (block-x)
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-x" in cp.stdout.decode()
            assert "Including block block-b" not in cp.stdout.decode()
            assert "no child" not in cp.stdout.decode()

            # Now make all blocks visible
            self.scenario.unmask()
            api.set_blocks(self.scenario.blocks)

            # Run again
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-c" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


@pytest.mark.order(ORDER)
class TestGenesis:
    """
    Start with empty, unconstrained db.
    """

    scenario = Scenario(SCENARIO_DESCRIPTION, 0, 1234560000000)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, unconstrained_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Bootstrapping step 1/2 - syncing core tables" in cp.stdout.decode()

            with pg.connect(unconstrained_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


def _test_db_state(conn: pg.Connection, s: Scenario):
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_erg_balances(cur, s)
        assert_erg_diffs(cur, s)
        assert_tokens_diffs(cur, s)
        assert_tokens_balances(cur, s)


def assert_db_constraints(conn: pg.Connection):
    # Erg bal
    assert_pk(conn, "bal", "erg", ["address"])
    assert_column_not_null(conn, "bal", "erg", "address")
    assert_column_not_null(conn, "bal", "erg", "value")
    assert_column_ge(conn, "bal", "erg", "value", 0)
    assert_index(conn, "bal", "erg", "erg_value_idx")

    # Erg diffs
    assert_pk(conn, "bal", "erg_diffs", ["address", "height", "tx_id"])
    assert_column_not_null(conn, "bal", "erg_diffs", "address")
    assert_column_not_null(conn, "bal", "erg_diffs", "height")
    assert_column_not_null(conn, "bal", "erg_diffs", "tx_id")
    assert_column_not_null(conn, "bal", "erg_diffs", "value")
    assert_index(conn, "bal", "erg_diffs", "erg_diffs_height_idx")

    # Tokens bal
    assert_pk(conn, "bal", "tokens", ["address", "token_id"])
    assert_column_not_null(conn, "bal", "tokens", "address")
    assert_column_not_null(conn, "bal", "tokens", "token_id")
    assert_column_not_null(conn, "bal", "tokens", "value")
    assert_column_ge(conn, "bal", "tokens", "value", 0)
    assert_index(conn, "bal", "tokens", "tokens_value_idx")

    # Tokens diffs
    assert_pk(conn, "bal", "tokens_diffs", ["address", "token_id", "height", "tx_id"])
    assert_column_not_null(conn, "bal", "tokens_diffs", "address")
    assert_column_not_null(conn, "bal", "tokens_diffs", "token_id")
    assert_column_not_null(conn, "bal", "tokens_diffs", "height")
    assert_column_not_null(conn, "bal", "tokens_diffs", "tx_id")
    assert_column_not_null(conn, "bal", "tokens_diffs", "value")
    assert_index(conn, "bal", "tokens_diffs", "tokens_diffs_height_idx")


def assert_erg_balances(cur: pg.Cursor, s: Scenario):
    cur.execute("select address, value from bal.erg order by 1;")
    rows = cur.fetchall()
    assert len(rows) == 5
    assert rows[0] == (s.address("base"), 950)
    assert rows[1] == (s.address("con1"), 1)
    assert rows[2] == (s.address("con2"), 40)
    assert rows[3] == (s.address("pub1"), 7)
    assert rows[4] == (s.address("pub2"), 2)


def assert_erg_diffs(cur: pg.Cursor, s: Scenario):
    h = s.parent_height
    cur.execute(
        "select height, tx_id, address, value from bal.erg_diffs order by 1, 2, 4;"
    )
    rows = cur.fetchall()
    assert len(rows) == 13

    bootstrap_tx_id = GENESIS_ID if s.parent_height == 0 else "bootstrap-tx"
    assert rows[0] == (h + 0, bootstrap_tx_id, s.address("base"), 1000)

    assert rows[1] == (h + 1, s.id("tx-a1"), s.address("base"), -50)
    assert rows[2] == (h + 1, s.id("tx-a1"), s.address("con1"), 50)

    assert rows[3] == (h + 2, s.id("tx-b1"), s.address("con1"), -50)
    assert rows[4] == (h + 2, s.id("tx-b1"), s.address("pub1"), 10)
    assert rows[5] == (h + 2, s.id("tx-b1"), s.address("con2"), 40)

    assert rows[6] == (h + 3, s.id("tx-c1"), s.address("pub1"), -4)
    assert rows[7] == (h + 3, s.id("tx-c1"), s.address("fees"), 1)
    assert rows[8] == (h + 3, s.id("tx-c1"), s.address("pub2"), 3)

    assert rows[9] == (h + 3, s.id("tx-c2"), s.address("fees"), -1)
    assert rows[10] == (h + 3, s.id("tx-c2"), s.address("con1"), 1)

    assert rows[11] == (h + 3, s.id("tx-c3"), s.address("pub2"), -1)
    assert rows[12] == (h + 3, s.id("tx-c3"), s.address("pub1"), 1)


def assert_tokens_balances(cur: pg.Cursor, s: Scenario):
    cur.execute("select address, token_id, value from bal.tokens order by 1, 2;")
    rows = cur.fetchall()
    assert len(rows) == 2
    assert rows[0] == (s.address("pub1"), s.id("con1-box1"), 1600)
    assert rows[1] == (s.address("pub2"), s.id("con1-box1"), 400)


def assert_tokens_diffs(cur: pg.Cursor, s: Scenario):
    h = s.parent_height
    cur.execute(
        "select height, tx_id, address, token_id, value from bal.tokens_diffs order by 1, 2, 3;"
    )
    rows = cur.fetchall()
    assert len(rows) == 7
    assert rows[0] == (h + 2, s.id("tx-b1"), s.address("pub1"), s.id("con1-box1"), 2000)

    assert rows[1] == (h + 3, s.id("tx-c1"), s.address("pub1"), s.id("con1-box1"), -500)
    assert rows[2] == (h + 3, s.id("tx-c1"), s.address("pub1"), s.id("pub1-box1"), 50)
    assert rows[3] == (h + 3, s.id("tx-c1"), s.address("pub2"), s.id("con1-box1"), 500)
    assert rows[4] == (h + 3, s.id("tx-c3"), s.address("pub1"), s.id("con1-box1"), 100)
    assert rows[5] == (h + 3, s.id("tx-c3"), s.address("pub1"), s.id("pub1-box1"), -50)
    assert rows[6] == (h + 3, s.id("tx-c3"), s.address("pub2"), s.id("con1-box1"), -100)

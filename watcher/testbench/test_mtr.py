import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil
from fixtures.scenario import Scenario
from fixtures.config import temp_cfg
from fixtures.db import bootstrap_db
from fixtures.db import fill_rev0_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import temp_db_rev0_class_scoped
from fixtures.db import unconstrained_db_class_scoped
from utils import assert_column_not_null, run_watcher
from utils import assert_pk

ORDER = 14

SCENARIO_DESCRIPTION = """
    block-a // coinbase tx:
        base-box1 1000
        >
        base-box2  950
        con1-box1   50

    //----------------------fork-of-b----------------------
    block-x // fork of block b to be ignored/rolled back:
        con1-box1   50
        >
        con9-box1   30
        pub9-box1   20 (con1-box1: 3000)
        --
        // extra tx in orphan block
        con9-box1   30
        > 
        con9-box2   30
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
        pub1-box2    5 (con1-box1: 1500)
        pub2-box1    4 ( 500 con1-box1)
        fees-box1    1
        --
        fees-box1    1
        >
        con1-box2    1
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

    scenario = Scenario(SCENARIO_DESCRIPTION, 0, Scenario.GENESIS_TIMESTAMP + 100_000)

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


@pytest.mark.order(ORDER)
class TestMigrations:
    """
    Aplly migration to synced db
    """

    scenario = Scenario(SCENARIO_DESCRIPTION, 599_999, 1234560000000, main_only=True)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_rev0_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)

            # Prepare db
            with pg.connect(temp_db_rev0_class_scoped) as conn:
                fill_rev0_db(conn, self.scenario)

            # Run
            cp = run_watcher(temp_cfg, allow_migrations=True)
            assert cp.returncode == 0
            assert "Applying migration 1" in cp.stdout.decode()

            with pg.connect(temp_db_rev0_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


def _test_db_state(conn: pg.Connection, s: Scenario):
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_utxos(cur, s)
        assert_transactions(cur, s)


def assert_db_constraints(conn: pg.Connection):
    # Utxos
    assert_pk(conn, "mtr", "utxos", ["height"])

    # Transactions
    assert_pk(conn, "mtr", "transactions", ["height"])
    assert_column_not_null(conn, "mtr", "transactions", "height")
    assert_column_not_null(conn, "mtr", "transactions", "daily_1d")
    assert_column_not_null(conn, "mtr", "transactions", "daily_7d")
    assert_column_not_null(conn, "mtr", "transactions", "daily_28d")


def assert_transactions(cur: pg.Cursor, s: Scenario):
    cur.execute(
        "select height, daily_1d, daily_7d, daily_28d from mtr.transactions order by 1;"
    )
    rows = cur.fetchall()
    assert len(rows) == 4
    assert rows[0] == (s.parent_height + 0, 1, 0, 0)  # initial state
    assert rows[1] == (s.parent_height + 1, 2, 0, 0)  # +1 tx
    assert rows[2] == (s.parent_height + 2, 3, 0, 0)  # +1 tx
    assert rows[3] == (s.parent_height + 3, 5, 1, 0)  # +2 txs


def assert_utxos(cur: pg.Cursor, s: Scenario):
    cur.execute("select height, value from mtr.utxos order by 1;")
    rows = cur.fetchall()
    assert len(rows) == 4
    assert rows[0] == (s.parent_height + 0, 1)  # initial state
    assert rows[1] == (s.parent_height + 1, 2)  # spend 1 create 2 (+1)
    assert rows[2] == (s.parent_height + 2, 3)  # spend 1 create 2 (+1)
    assert rows[3] == (s.parent_height + 3, 5)  # spend 2 create 4 (+2)

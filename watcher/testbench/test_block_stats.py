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
        pub1-box1   10

    block-c
        pub1-box1   10
        >
        pub1-box2    5
        pub2-box1    4
        fees-box1    1
        --
        pub1-box2    5
        >
        pub1-box3    3
        fees-box2    2
        --
        fees-box1    1
        fees-box2    2
        >
        con1-box2    3
    """


@pytest.mark.order(ORDER)
class TestSync:
    """
    Start with bootstrapped db.
    """

    scenario = Scenario(SCENARIO_DESCRIPTION, 99_999, 1234560000000)

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

    scenario = Scenario(SCENARIO_DESCRIPTION, 99_999, 1234560000000)

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

    scenario = Scenario(SCENARIO_DESCRIPTION, 99_999, 1234560000000)

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

    scenario = Scenario(SCENARIO_DESCRIPTION, 99_999, 1234560000000, main_only=True)

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
        assert_circulating_supply(cur, s)
        assert_emission(cur, s)
        assert_rewards(cur, s)
        assert_tx_fees(cur, s)
        assert_tx_count(cur, s)
        assert_volume(cur, s)


def assert_db_constraints(conn: pg.Connection):
    assert_pk(conn, "blk", "stats", ["height"])
    assert_column_not_null(conn, "blk", "stats", "height")
    assert_column_not_null(conn, "blk", "stats", "circulating_supply")
    assert_column_not_null(conn, "blk", "stats", "emission")
    assert_column_not_null(conn, "blk", "stats", "reward")
    assert_column_not_null(conn, "blk", "stats", "tx_fees")
    assert_column_not_null(conn, "blk", "stats", "tx_fees")
    assert_column_not_null(conn, "blk", "stats", "tx_count")
    assert_column_not_null(conn, "blk", "stats", "volume")


def assert_circulating_supply(cur: pg.Cursor, s: Scenario):
    cur.execute("select height, circulating_supply from blk.stats order by 1;")
    rows = cur.fetchall()
    assert len(rows) == 4
    if s.parent_height == 0:
        assert rows[0] == (0, 0)
        assert rows[1] == (1, 75_000_000_000 * 1)
        assert rows[2] == (2, 75_000_000_000 * 2)
        assert rows[3] == (3, 75_000_000_000 * 3)
    else:
        assert rows[0] == (s.parent_height + 0, 75_000_000_000 * 1)
        assert rows[1] == (s.parent_height + 1, 75_000_000_000 * 2)
        assert rows[2] == (s.parent_height + 2, 75_000_000_000 * 3)
        assert rows[3] == (s.parent_height + 3, 75_000_000_000 * 4)


def assert_emission(cur: pg.Cursor, s: Scenario):
    cur.execute("select height, emission from blk.stats order by 1;")
    rows = cur.fetchall()
    assert len(rows) == 4
    if s.parent_height == 0:
        assert rows[0] == (0, 0)
        assert rows[1] == (1, 75_000_000_000)
        assert rows[2] == (2, 75_000_000_000)
        assert rows[3] == (3, 75_000_000_000)
    else:
        assert rows[0] == (s.parent_height + 0, 75_000_000_000)
        assert rows[1] == (s.parent_height + 1, 75_000_000_000)
        assert rows[2] == (s.parent_height + 2, 75_000_000_000)
        assert rows[3] == (s.parent_height + 3, 75_000_000_000)


def assert_rewards(cur: pg.Cursor, s: Scenario):
    cur.execute("select height, reward from blk.stats order by 1;")
    rows = cur.fetchall()
    assert len(rows) == 4
    if s.parent_height == 0:
        assert rows[0] == (0, 0)
        assert rows[1] == (1, 67_500_000_000)
        assert rows[2] == (2, 67_500_000_000)
        assert rows[3] == (3, 67_500_000_000)
    else:
        assert rows[0] == (s.parent_height + 0, 67_500_000_000)
        assert rows[1] == (s.parent_height + 1, 67_500_000_000)
        assert rows[2] == (s.parent_height + 2, 67_500_000_000)
        assert rows[3] == (s.parent_height + 3, 67_500_000_000)


def assert_tx_fees(cur: pg.Cursor, s: Scenario):
    cur.execute("select height, tx_fees from blk.stats order by 1;")
    rows = cur.fetchall()
    assert len(rows) == 4
    assert rows[0] == (s.parent_height + 0, 0)
    assert rows[1] == (s.parent_height + 1, 0)
    assert rows[2] == (s.parent_height + 2, 0)
    assert rows[3] == (s.parent_height + 3, 3)


def assert_tx_count(cur: pg.Cursor, s: Scenario):
    cur.execute("select height, tx_count from blk.stats order by 1;")
    rows = cur.fetchall()
    assert len(rows) == 4
    assert rows[0] == (s.parent_height + 0, 1)
    assert rows[1] == (s.parent_height + 1, 1)
    assert rows[2] == (s.parent_height + 2, 1)
    assert rows[3] == (s.parent_height + 3, 3)


def assert_volume(cur: pg.Cursor, s: Scenario):
    cur.execute("select height, volume from blk.stats order by 1;")
    rows = cur.fetchall()
    assert len(rows) == 4
    assert rows[0] == (s.parent_height + 0, 0)
    assert rows[1] == (s.parent_height + 1, 0)
    assert rows[2] == (s.parent_height + 2, 50)
    assert rows[3] == (s.parent_height + 3, 5 + 2)

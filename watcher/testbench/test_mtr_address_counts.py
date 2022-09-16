from fixtures import scenario
import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil
from fixtures.scenario import Scenario
from fixtures.scenario.genesis import GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.db import bootstrap_db as _bootstrap_db
from fixtures.db import fill_rev0_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import temp_db_rev0_class_scoped
from fixtures.db import unconstrained_db_class_scoped
from fixtures.scenario.addresses import AddressCatalogue as AC
from utils import run_watcher
from utils import assert_pk
from utils import assert_column_not_null
from utils import assert_column_ge


ORDER = 13


def wrapped_bootstrap_db(conn, scenario):
    """
    Used to change some sql default before calling bootstrap_db
    """
    import fixtures.db.sql

    # Override value assigned to existing inputs
    fixtures.db.sql.DEFAULT_BOX_VALUE = 2_000_000_000_000_000
    _bootstrap_db(conn, scenario)


SCENARIO_DESCRIPTION = f"""
    block-a
        // Existing base box with same value as fixtures.db.sql.default_box_value
        base-box1 {2_000_000 * 10**9}
        >
        p2re-box1 {1499999 * 10**9} // pay to reemission contract, should be ignored
        min1-box1       {1 * 10**9}
        pub1-box1  {500000 * 10**9}
        pub2-box1  {500000 * 10**9}
    block-b
        pub1-box1 {500000 * 10**9}
        >
        pub1-box2 {300000 * 10**9}
        pub3-box1  {90000 * 10**9}
        pub4-box1  {10000 * 10**9}
        con1-box1 {100000 * 10**9}
    block-x
        pub1-box2 {300000 * 10**9}
        >
        con9-box1 {300000 * 10**9}
    block-c-b
        // Spend pub entirely
        pub4-box1 {10000 * 10**9}
        >
        pub2-box2 {10000 * 10**9}
        --
        pub3-box1 {90000 * 10**9}
        >
        pub3-box2 {89899.999 * 10**9}
        pub5-box1     {0.001 * 10**9}
        pub6-box1   {100 * 10**9}
    block-d
        con1-box1 {100000 * 10**9}
        >
        con1-box2 {98999 * 10**9}
        con2-box1  {1000 * 10**9}
        // should be ignored
        reem-box1     {1 * 10**9}

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
                wrapped_bootstrap_db(conn, self.scenario)

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-a" in cp.stdout.decode()

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

            # Initially have chain a-b-c-x
            self.scenario.mask(4)
            api.set_blocks(self.scenario.blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                wrapped_bootstrap_db(conn, self.scenario)

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
            assert "Including block block-d" in cp.stdout.decode()

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
        assert_p2pk_counts(cur, s)
        assert_contract_counts(cur, s)


def assert_db_constraints(conn: pg.Connection):
    table = "address_counts_by_balance_p2pk"
    assert_pk(conn, "mtr", table, ["height"])
    assert_column_not_null(conn, "mtr", table, "height")
    assert_column_not_null(conn, "mtr", table, "total")
    assert_column_not_null(conn, "mtr", table, "ge_0p001")
    assert_column_not_null(conn, "mtr", table, "ge_0p01")
    assert_column_not_null(conn, "mtr", table, "ge_0p1")
    assert_column_not_null(conn, "mtr", table, "ge_1")
    assert_column_not_null(conn, "mtr", table, "ge_10")
    assert_column_not_null(conn, "mtr", table, "ge_100")
    assert_column_not_null(conn, "mtr", table, "ge_1k")
    assert_column_not_null(conn, "mtr", table, "ge_10k")
    assert_column_not_null(conn, "mtr", table, "ge_100k")
    assert_column_not_null(conn, "mtr", table, "ge_1m")

    table = "address_counts_by_balance_contracts"
    assert_pk(conn, "mtr", table, ["height"])
    assert_column_not_null(conn, "mtr", table, "height")
    assert_column_not_null(conn, "mtr", table, "total")
    assert_column_not_null(conn, "mtr", table, "ge_0p001")
    assert_column_not_null(conn, "mtr", table, "ge_0p01")
    assert_column_not_null(conn, "mtr", table, "ge_0p1")
    assert_column_not_null(conn, "mtr", table, "ge_1")
    assert_column_not_null(conn, "mtr", table, "ge_10")
    assert_column_not_null(conn, "mtr", table, "ge_100")
    assert_column_not_null(conn, "mtr", table, "ge_1k")
    assert_column_not_null(conn, "mtr", table, "ge_10k")
    assert_column_not_null(conn, "mtr", table, "ge_100k")
    assert_column_not_null(conn, "mtr", table, "ge_1m")

    table = "address_counts_by_balance_miners"
    assert_pk(conn, "mtr", table, ["height"])
    assert_column_not_null(conn, "mtr", table, "height")
    assert_column_not_null(conn, "mtr", table, "total")
    assert_column_not_null(conn, "mtr", table, "ge_0p001")
    assert_column_not_null(conn, "mtr", table, "ge_0p01")
    assert_column_not_null(conn, "mtr", table, "ge_0p1")
    assert_column_not_null(conn, "mtr", table, "ge_1")
    assert_column_not_null(conn, "mtr", table, "ge_10")
    assert_column_not_null(conn, "mtr", table, "ge_100")
    assert_column_not_null(conn, "mtr", table, "ge_1k")
    assert_column_not_null(conn, "mtr", table, "ge_10k")
    assert_column_not_null(conn, "mtr", table, "ge_100k")
    assert_column_not_null(conn, "mtr", table, "ge_1m")


def assert_p2pk_counts(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select height
            , total
            , ge_0p001
            , ge_0p01
            , ge_0p1
            , ge_1
            , ge_10
            , ge_100
            , ge_1k
            , ge_10k
            , ge_100k
            , ge_1m
        from mtr.address_counts_by_balance_p2pk
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 5
    assert rows[0] == (s.parent_height + 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    assert rows[1] == (s.parent_height + 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0)
    assert rows[2] == (s.parent_height + 2, 4, 4, 4, 4, 4, 4, 4, 4, 4, 2, 0)
    assert rows[3] == (s.parent_height + 3, 5, 5, 4, 4, 4, 4, 4, 3, 3, 2, 0)
    assert rows[4] == (s.parent_height + 4, 5, 5, 4, 4, 4, 4, 4, 3, 3, 2, 0)


def assert_contract_counts(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select height
            , total
            , ge_0p001
            , ge_0p01
            , ge_0p1
            , ge_1
            , ge_10
            , ge_100
            , ge_1k
            , ge_10k
            , ge_100k
            , ge_1m
        from mtr.address_counts_by_balance_contracts
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 5
    assert rows[0] == (s.parent_height + 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    assert rows[1] == (s.parent_height + 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    assert rows[2] == (s.parent_height + 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0)
    assert rows[3] == (s.parent_height + 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0)
    assert rows[4] == (s.parent_height + 4, 2, 2, 2, 2, 2, 2, 2, 2, 1, 0, 0)


def assert_miner_counts(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select height
            , total
            , ge_0p001
            , ge_0p01
            , ge_0p1
            , ge_1
            , ge_10
            , ge_100
            , ge_1k
            , ge_10k
            , ge_100k
            , ge_1m
        from mtr.address_counts_by_balance_miners
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 5
    assert rows[0] == (s.parent_height + 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    assert rows[1] == (s.parent_height + 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0)
    assert rows[2] == (s.parent_height + 2, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0)
    assert rows[3] == (s.parent_height + 3, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0)
    assert rows[4] == (s.parent_height + 4, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0)

from fixtures import scenario
import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil
from fixtures.scenario import Scenario
from fixtures.scenario.genesis import GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.db import bootstrap_db
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


SCENARIO_DESCRIPTION = """
    
    block-a
        base-box1 1000
        >
        base-box2  900
        con1-box1  100
        tres-box1    0

    //----------------------fork-of-b----------------------
    block-x
        base-box2  900
        >
        base-box9  800
        con1-box9  100
    //------------------------------------------------------

    block-b-a
        base-box2  900
        >
        base-box3  800
        con1-box2  100

    block-c
        base-box3  800
        >
        base-box4  700
        con1-box3  100

    block-d
        base-box4  700
        >
        base-box5  600
        con1-box4  100
    """

# Timestamp of first block, in the future (~2037)
# to avoid triggering calls to (mocked) coingeck api.
T0 = 2123450000000

# Number of ms between two blocks of consecutive heights
DT = scenario.TIMESTAMP_INTERVAL


def prefill_coingecko_data(db: pg.Connection):
    """Inserts coingecko data into test db."""
    with db.cursor() as cur:
        cur.execute(
            f"""
            insert into cgo.ergusd (timestamp, value) values
                -- First block should be linearly interpolated between 100 and 200
                ({T0 - 0.4 * DT}, 100.),
                ({T0 + 0.4 * DT}, 200.),
                -- Seconds block should be spot on
                ({T0 + 1.0 * DT}, 300.),
                ({T0 + 1.5 * DT}, 400.),
                -- Third block should be linearly interpolated between 500 and 600
                ({T0 + 1.9 * DT}, 500.),
                ({T0 + 2.3 * DT}, 600.);
                -- Fourth block should have latest available value, provisionally
        """
        )


@pytest.mark.order(ORDER)
class TestSync:
    """
    Start with bootstrapped db.
    """

    scenario = Scenario(SCENARIO_DESCRIPTION, 599_999, T0)

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
                prefill_coingecko_data(conn)

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

    scenario = Scenario(SCENARIO_DESCRIPTION, 599_999, T0)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi() as api:
            api = ApiUtil()

            # Initially have chain a-x
            self.scenario.mask(2)
            api.set_blocks(self.scenario.blocks)

            # Bootstrap db
            with pg.connect(temp_db_class_scoped) as conn:
                bootstrap_db(conn, self.scenario)
                prefill_coingecko_data(conn)
                # Tweak value at height 600001 to check rollback works as expected
                conn.execute(
                    f"update cgo.ergusd set value = 10 * value where timestamp = {T0 + DT};"
                )

            # Run to include block x
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-x" in cp.stdout.decode()

            # Now make all blocks visible
            self.scenario.unmask()
            api.set_blocks(self.scenario.blocks)

            # And change value at 600001 back to what it was
            with pg.connect(temp_db_class_scoped) as conn:
                conn.execute(
                    f"update cgo.ergusd set value = 0.1 * value where timestamp = {T0 + DT};"
                )

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

    scenario = Scenario(SCENARIO_DESCRIPTION, 0, T0)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, unconstrained_db_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)

            # Edit db
            with pg.connect(unconstrained_db_class_scoped) as conn:
                prefill_coingecko_data(conn)

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

    scenario = Scenario(SCENARIO_DESCRIPTION, 599_999, T0, main_only=True)

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
        _test_db_state(synced_db, self.scenario, migrations=True)


def _test_db_state(conn: pg.Connection, s: Scenario, migrations=False):
    """
    Can't inject coingecko data for TestMigrations, so assert differently.
    """
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_ergusd(cur, s, migrations)
        assert_ergusd_provisional(cur, s, migrations)


def assert_db_constraints(conn: pg.Connection):
    # mtr.ergusd
    assert_pk(conn, "mtr", "ergusd", ["height"])
    assert_column_not_null(conn, "mtr", "ergusd", "height")
    assert_column_not_null(conn, "mtr", "ergusd", "value")
    # mtr.ergusd_provisional
    assert_pk(conn, "mtr", "ergusd_provisional", ["height"])
    assert_column_not_null(conn, "mtr", "ergusd_provisional", "height")


def assert_ergusd(cur: pg.Cursor, s: Scenario, migrations: bool):
    cur.execute(
        """
        select height
            , value
        from mtr.ergusd
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 5
    if migrations:
        # Mock coingecko returns 10 for all timestamps
        assert rows[0] == (s.parent_height + 0, 10)  # genesis
        assert rows[1] == (s.parent_height + 1, 10)  # block a
        assert rows[2] == (s.parent_height + 2, 10)  # block b
        assert rows[3] == (s.parent_height + 3, 10)  # block c
        assert rows[4] == (s.parent_height + 4, 10)  # block d
    else:
        assert rows[0] == (s.parent_height + 0, 100)  # genesis
        assert rows[1] == (s.parent_height + 1, 150)  # block a
        assert rows[2] == (s.parent_height + 2, 300)  # block b
        assert rows[3] == (s.parent_height + 3, 525)  # block c
        assert rows[4] == (s.parent_height + 4, 600)  # block d


def assert_ergusd_provisional(cur: pg.Cursor, s: Scenario, migrations: bool):
    cur.execute(
        """
        select height
        from mtr.ergusd_provisional
        order by 1;
        """
    )
    rows = cur.fetchall()
    if migrations:
        assert len(rows) == 5
        assert rows[0] == (s.parent_height + 0,)  # genesis
        assert rows[1] == (s.parent_height + 1,)  # block a
        assert rows[2] == (s.parent_height + 2,)  # block b
        assert rows[3] == (s.parent_height + 3,)  # block c
        assert rows[4] == (s.parent_height + 4,)  # block d
    else:
        assert len(rows) == 1
        assert rows[0] == (s.parent_height + 4,)  # block d

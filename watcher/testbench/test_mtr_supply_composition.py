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
        // coinbase tx:
        base-box1 1000
        >
        base-box2  190
        tres-box1   50 // treasury box
        cex1-box1   50 // main cex
        pub1-box1  100 // cex1 deposit address, not known yet
        pub2-box1  200  
        pub3-box1  200  
        pub4-box1  200
        min1-box1   10

    block-b
        // Sell cex 1 deposit. Now pub1 is a known cex deposit address.
        pub1-box1  100
        >
        cex1-box2  100
        --
        // Move some from p2pk to contracts
        pub2-box1  200
        >
        con1-box1  200

    block-c
        min1-box1   10
        >
        con1-box2    2
        min1-box2    6
        reem-box1    1
        p2re-box1    1

    // ----------------------fork-of-d----------------------
    block-x // fork of block d to be ignored/rolled back:
        base-box2  190
        >
        con2-box1  190
    //------------------------------------------------------

    block-d-c
        base-box2  190
        >
        con3-box1  100
        base-box3   90

    //one more block to tell d and x appart and have enough to trigger a repair event
    block-e
        base-box3   90
        >
        base-box4   89
        con4-box1    1
    """


@pytest.mark.order(ORDER)
class TestSync:
    """
    Start with bootstrapped db.
    """

    # Using height of 899k to have 0 ERG treasury rewards
    scenario = Scenario(SCENARIO_DESCRIPTION, 899_999, 1234560000000)

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

    # Using height of 899k to have 0 ERG treasury rewards
    scenario = Scenario(SCENARIO_DESCRIPTION, 899_999, 1234560000000)

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

    # Using height of 199k to have 7.5 ERG treasury rewards
    scenario = Scenario(SCENARIO_DESCRIPTION, 199_999, 1234560000000, main_only=True)

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
        assert_supply_composition(cur, s)
        assert_summary(cur, s)


def assert_db_constraints(conn: pg.Connection):
    assert_pk(conn, "mtr", "supply_composition", ["height"])
    assert_column_not_null(conn, "mtr", "supply_composition", "p2pks")
    assert_column_not_null(conn, "mtr", "supply_composition", "cex_main")
    assert_column_not_null(conn, "mtr", "supply_composition", "cex_deposits")
    assert_column_not_null(conn, "mtr", "supply_composition", "contracts")
    assert_column_not_null(conn, "mtr", "supply_composition", "miners")
    assert_column_not_null(conn, "mtr", "supply_composition", "treasury")


def assert_supply_composition(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select height
            , p2pks 
            , cex_main
            , cex_deposits
            , contracts
            , miners
            , treasury
        from mtr.supply_composition
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 6
    ph = s.parent_height
    tr = initial_treasury_reward()
    pr = parent_reward(ph)
    treas = lambda i: 0
    #  For genesis and migration scenarions
    if s.parent_height < 200_000:
        treas = lambda i: pr + i * tr
    assert rows[0] == (ph + 0, 0, 0, 0, 0, 0, pr)
    assert rows[1] == (ph + 1, 600, 50, 100, 0, 10, 50 + treas(1))
    assert rows[2] == (ph + 2, 400, 150, 0, 200, 10, 50 + treas(2))
    assert rows[3] == (ph + 3, 400, 150, 0, 202, 6, 50 + treas(3))
    assert rows[4] == (ph + 4, 400, 150, 0, 302, 6, 50 + treas(4))
    assert rows[5] == (ph + 5, 400, 150, 0, 303, 6, 50 + treas(5))


def assert_summary(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select label, current, diff_1d, diff_1w, diff_4w, diff_6m, diff_1y
        from mtr.supply_composition_summary;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 7
    ph = s.parent_height
    tr = initial_treasury_reward()
    pr = parent_reward(ph)
    assert rows[0] == ("p2pks", 400, 400, 400, 400, 400, 400)
    assert rows[1] == ("cex_main", 150, 150, 150, 150, 150, 150)
    assert rows[2] == ("cex_deposits", 0, 0, 0, 0, 0, 0)
    assert rows[3] == ("contracts", 303, 303, 303, 303, 303, 303)
    assert rows[4] == ("miners", 6, 6, 6, 6, 6, 6)
    t = 50
    #  For genesis and migration scenarions
    if s.parent_height < 200_000:
        t = t = 50 + pr + 5 * tr
    assert rows[5] == ("treasury", t, t - pr, t - pr, t - pr, t - pr, t - pr)
    assert rows[6] == (
        "total",
        859 + t,
        859 + t - pr,
        859 + t - pr,
        859 + t - pr,
        859 + t - pr,
        859 + t - pr,
    )


def initial_treasury_reward() -> int:
    return 75 * 10**8  # 7.5 ERG treasury reward


def parent_reward(parent_height: int) -> int:
    # Treasury reward in first block is zero for h = 0
    # but will be non-zero otherwise
    if parent_height == 0:
        return 0
    elif parent_height <= 526_000:
        return initial_treasury_reward()
    else:
        # Non-genesis scenario heights are high enough to not
        # worry about decreasing treasury rewards
        return 0

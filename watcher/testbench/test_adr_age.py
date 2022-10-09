# Extra test for adr.erg mean_age_timestamps
import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil
from fixtures.scenario import Scenario
from fixtures.scenario.genesis import GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.db import bootstrap_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import temp_db_rev0_class_scoped
from fixtures.db import unconstrained_db_class_scoped
from fixtures.db import fill_rev0_db

from test_mtr_cex import SCENARIO_DESCRIPTION
from utils import run_watcher
from utils import assert_pk
from utils import assert_index
from utils import assert_column_ge
from utils import assert_column_not_null


ORDER = 12

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

    scenario = Scenario(SCENARIO_DESCRIPTION, 599_999, 1234560000000, main_only=True)

    @pytest.fixture(scope="class")
    def synced_db(self, temp_cfg, temp_db_rev0_class_scoped):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi():
            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)

            # Prepare db
            with pg.connect(temp_db_rev0_class_scoped) as conn:
                fill_rev0_db(conn, self.scenario)

            # Run
            cp = run_watcher(temp_cfg, allow_migrations=True)
            assert cp.returncode == 0
            assert "Applying migration 1" in cp.stdout.decode()
            assert "Applying migration 2" in cp.stdout.decode()

            with pg.connect(temp_db_rev0_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


def _test_db_state(conn: pg.Connection, s: Scenario):
    with conn.cursor() as cur:
        assert_erg_balances(cur, s)
        assert_erg_diffs(cur, s)
        assert_erg_mean_age_timestamps(cur, s)


def assert_erg_balances(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select a.address
            , b.value
        from adr.erg b
        join core.addresses a on a.id = b.address_id
        order by a.id;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 11
    assert rows[0] == (s.address("base"), 89)
    assert rows[1] == (s.address("tres"), 50)
    assert rows[2] == (s.address("cex1"), 150)
    assert rows[3] == (s.address("pub3"), 200)
    assert rows[4] == (s.address("pub4"), 200)
    assert rows[5] == (s.address("min1"), 6)
    assert rows[6] == (s.address("con1"), 202)
    assert rows[7] == (s.address("reem"), 1)
    assert rows[8] == (s.address("p2re"), 1)
    assert rows[9] == (s.address("con3"), 100)
    assert rows[10] == (s.address("con4"), 1)


def assert_erg_diffs(cur: pg.Cursor, s: Scenario):
    h = s.parent_height
    cur.execute(
        """
        select d.height
            , d.tx_id
            , a.address
            , d.value
        from adr.erg_diffs d
        join core.addresses a on a.id = d.address_id
        order by 1, 2, d.value, a.id;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 21

    bootstrap_tx_id = GENESIS_ID if s.parent_height == 0 else "bootstrap-tx"
    assert rows[0] == (h + 0, bootstrap_tx_id, s.address("base"), 1000)

    assert rows[1] == (h + 1, s.id("tx-a1"), s.address("base"), -810)
    assert rows[2] == (h + 1, s.id("tx-a1"), s.address("min1"), 10)
    assert rows[3] == (h + 1, s.id("tx-a1"), s.address("tres"), 50)
    assert rows[4] == (h + 1, s.id("tx-a1"), s.address("cex1"), 50)
    assert rows[5] == (h + 1, s.id("tx-a1"), s.address("pub1"), 100)
    assert rows[6] == (h + 1, s.id("tx-a1"), s.address("pub2"), 200)
    assert rows[7] == (h + 1, s.id("tx-a1"), s.address("pub3"), 200)
    assert rows[8] == (h + 1, s.id("tx-a1"), s.address("pub4"), 200)

    assert rows[9] == (h + 2, s.id("tx-b1"), s.address("pub1"), -100)
    assert rows[10] == (h + 2, s.id("tx-b1"), s.address("cex1"), 100)

    assert rows[11] == (h + 2, s.id("tx-b2"), s.address("pub2"), -200)
    assert rows[12] == (h + 2, s.id("tx-b2"), s.address("con1"), 200)

    assert rows[13] == (h + 3, s.id("tx-c1"), s.address("min1"), -4)
    assert rows[14] == (h + 3, s.id("tx-c1"), s.address("reem"), 1)
    assert rows[15] == (h + 3, s.id("tx-c1"), s.address("p2re"), 1)
    assert rows[16] == (h + 3, s.id("tx-c1"), s.address("con1"), 2)

    assert rows[17] == (h + 4, s.id("tx-d1"), s.address("base"), -100)
    assert rows[18] == (h + 4, s.id("tx-d1"), s.address("con3"), 100)

    assert rows[19] == (h + 5, s.id("tx-e1"), s.address("base"), -1)
    assert rows[20] == (h + 5, s.id("tx-e1"), s.address("con4"), 1)


def assert_erg_mean_age_timestamps(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select a.address
            , b.mean_age_timestamp
        from adr.erg b
        join core.addresses a on a.id = b.address_id
        order by a.id;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 11
    dt = 100_000
    ta = s.parent_ts + 1 * dt
    tb = s.parent_ts + 2 * dt
    tc = s.parent_ts + 3 * dt
    td = s.parent_ts + 4 * dt
    te = s.parent_ts + 5 * dt
    assert rows[0] == (s.address("base"), s.parent_ts)
    assert rows[1] == (s.address("tres"), ta)
    assert rows[2] == (s.address("cex1"), round(50 / 150 * ta + 100 / 150 * tb))
    assert rows[3] == (s.address("pub3"), ta)
    assert rows[4] == (s.address("pub4"), ta)
    assert rows[5] == (s.address("min1"), ta)
    assert rows[6] == (s.address("con1"), round(200 / 202 * tb + 2 / 202 * tc))
    assert rows[7] == (s.address("reem"), tc)
    assert rows[8] == (s.address("p2re"), tc)
    assert rows[9] == (s.address("con3"), td)
    assert rows[10] == (s.address("con4"), te)

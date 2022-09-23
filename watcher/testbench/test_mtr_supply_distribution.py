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
        tres-box1   50 // treasury box, ignored
        cex1-box1   50 // main cex, ignored
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
        base-box4   90
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


@pytest.mark.order(ORDER)
class TestRepair:
    """
    Same as TestSync, but triggering a repair event after full sync.
    """

    # Start one block later so last block has height multiple of 5
    # and trigger a repair event.
    scenario = Scenario(SCENARIO_DESCRIPTION, 599_999 + 1, 1234560000000)

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
                # Simulate an interupted repair,
                # Should be cleaned up at startup.
                with conn.cursor() as cur:
                    cur.execute("insert into ew.repairs (started) select now();")
                    cur.execute("create schema repair_adr;")
                conn.commit()

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-e" in cp.stdout.decode()
            assert "Repairing 5 blocks (600001 to 600005)" in cp.stdout.decode()
            assert "Done repairing heights 600001 to 600005" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
                yield conn

    def test_db_state(self, synced_db: pg.Connection):
        _test_db_state(synced_db, self.scenario)


def _test_db_state(conn: pg.Connection, s: Scenario):
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_supply_distribution_p2pk(cur, s)
        assert_supply_distribution_contracts(cur, s)
        assert_supply_distribution_miners(cur, s)


def assert_db_constraints(conn: pg.Connection):
    for table in [
        "supply_on_top_addresses_p2pk",
        "supply_on_top_addresses_contracts",
        "supply_on_top_addresses_miners",
    ]:
        assert_pk(conn, "mtr", table, ["height"])
        assert_column_not_null(conn, "mtr", table, "height")
        assert_column_not_null(conn, "mtr", table, "top_1_prc")
        assert_column_not_null(conn, "mtr", table, "top_1k")
        assert_column_not_null(conn, "mtr", table, "top_100")
        assert_column_not_null(conn, "mtr", table, "top_10")


def assert_supply_distribution_p2pk(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select height
            , top_1_prc 
            , top_1k 
            , top_100 
            , top_10 
        from mtr.supply_on_top_addresses_p2pk
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    ph = s.parent_height
    assert len(rows) == 6
    assert rows[0] == (ph + 0, 0, 0, 0, 0)
    assert rows[1] == (ph + 1, 200, 700, 700, 700)
    assert rows[2] == (ph + 2, 200, 400, 400, 400)
    assert rows[3] == (ph + 3, 200, 400, 400, 400)
    assert rows[4] == (ph + 4, 200, 400, 400, 400)
    assert rows[5] == (ph + 5, 200, 400, 400, 400)


def assert_supply_distribution_contracts(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select height
            , top_1_prc 
            , top_1k 
            , top_100 
            , top_10 
        from mtr.supply_on_top_addresses_contracts
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    ph = s.parent_height
    assert len(rows) == 6
    assert rows[0] == (ph + 0, 0, 0, 0, 0)
    assert rows[1] == (ph + 1, 0, 0, 0, 0)
    assert rows[2] == (ph + 2, 200, 200, 200, 200)
    assert rows[3] == (ph + 3, 202, 202, 202, 202)
    assert rows[4] == (ph + 4, 202, 302, 302, 302)
    assert rows[5] == (ph + 5, 202, 302, 302, 302)


def assert_supply_distribution_miners(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select height
            , top_1_prc 
            , top_1k 
            , top_100 
            , top_10 
        from mtr.supply_on_top_addresses_miners
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    ph = s.parent_height
    assert len(rows) == 6
    assert rows[0] == (ph + 0, 0, 0, 0, 0)
    assert rows[1] == (ph + 1, 10, 10, 10, 10)
    assert rows[2] == (ph + 2, 10, 10, 10, 10)
    assert rows[3] == (ph + 3, 6, 6, 6, 6)
    assert rows[4] == (ph + 4, 6, 6, 6, 6)
    assert rows[5] == (ph + 5, 6, 6, 6, 6)

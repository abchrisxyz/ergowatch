from ensurepip import bootstrap
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
        _test_db_state(synced_db, self.scenario, bootstrapped=True)


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
        _test_db_state(synced_db, self.scenario, bootstrapped=True)


def _test_db_state(conn: pg.Connection, s: Scenario, bootstrapped=False):
    """
    Test outcomes can be different for cases that trigger bootstrapping code or
    a repair event. This is indicated through the *bootstrapped* flag.

    TestSync and SyncRollback trigger no bootstrap and no repair.
    TestGenesis and TestMigrations will bootstrap their cex schema.
    TestRepair does no bootstrap but ends with a repair and so produces
    the same state as TestGenesis and TestMigrations.
    """
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_timestamps(cur, s, bootstrapped)


def assert_db_constraints(conn: pg.Connection):
    assert_pk(conn, "mtr", "supply_age_timestamps", ["height"])
    assert_column_not_null(conn, "mtr", "supply_age_timestamps", "overall")
    assert_column_not_null(conn, "mtr", "supply_age_timestamps", "p2pks")
    assert_column_not_null(conn, "mtr", "supply_age_timestamps", "cexs")
    assert_column_not_null(conn, "mtr", "supply_age_timestamps", "contracts")
    assert_column_not_null(conn, "mtr", "supply_age_timestamps", "miners")
    assert_column_not_null(conn, "mtr", "supply_age_seconds", "overall")
    assert_column_not_null(conn, "mtr", "supply_age_seconds", "p2pks")
    assert_column_not_null(conn, "mtr", "supply_age_seconds", "cexs")
    assert_column_not_null(conn, "mtr", "supply_age_seconds", "contracts")
    assert_column_not_null(conn, "mtr", "supply_age_seconds", "miners")


def assert_timestamps(cur: pg.Cursor, s: Scenario, bootstrapped: bool):
    rows = cur.execute("select height, address_id, value from adr.erg_diffs")
    cur.execute(
        """
        select height
            , overall
            , p2pks
            , cexs
            , contracts
            , miners
        from mtr.supply_age_timestamps
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 6
    ph = s.parent_height
    for r in rows:
        print(r)
    dt = 100_000
    ta = s.parent_ts + 1 * dt
    tb = s.parent_ts + 2 * dt
    tc = s.parent_ts + 3 * dt
    td = s.parent_ts + 4 * dt
    te = s.parent_ts + 5 * dt
    # if bootstrapped:
    #     assert rows[0] == (ph + 0, 0, 0, 0, 0, 0)
    #     assert rows[1] == (ph + 1, 0, 0, 0, 0, 0)
    #     assert rows[2] == (ph + 2, 0, 0, 0, 0, 0)
    #     assert rows[3] == (ph + 3, 0, 0, 0, 0, 0)
    #     assert rows[4] == (ph + 4, 0, 0, 0, 0, 0)
    #     assert rows[5] == (ph + 5, 0, 0, 0, 0, 0)
    # else:
    assert rows[0] == (ph + 0, 0, 0, 0, 0, 0)
    assert rows[1] == (
        ph + 1,
        760 / 760 * ta,
        700 / 700 * ta,
        50 / 50 * ta,
        0,
        10 / 10 * ta,
    )
    overall_b = round(460 / 760 * ta + 300 / 760 * tb)
    assert rows[2] == (
        ph + 2,
        overall_b,
        400 / 400 * ta,
        round(50 / 150 * ta + 100 / 150 * tb),
        200 / 200 * tb,
        10 / 10 * ta,
    )
    # In block c, circulating supply drops from 760 to 758.
    # 2 get sent to reemission contracts, and 2 more from miners
    # to contracts.
    #
    # Calculating overall age timestamp like this:
    # overall_c = round(756 / 758 * overall_b + 2 / 758 * tc)
    # gives 1234560039898, slightly different from 1234560040106
    # produced by watcher. This is because the above assumes erg is
    # spend from all addresses while it is only spent from min1:
    overall_c = round(760 / 758 * overall_b - 4 / 758 * ta + 2 / 758 * tc)
    assert overall_c == 1234560040106
    assert rows[3] == (
        ph + 3,
        overall_c,
        360 / 360 * ta,
        round(50 / 150 * ta + 100 / 150 * tb),
        round(200 / 202 * tb + 2 / 202 * tc),
        6 / 6 * ta,
    )
    # In block d circulating supply goes from 758 to 858
    overall_d = round(758 / 858 * overall_c + 100 / 858 * td)
    assert overall_d == 1234560070397
    # Correct for rounding diffs between python and pg
    overall_d -= 1
    assert overall_d == 1234560070396
    assert rows[4] == (
        ph + 4,
        overall_d,
        360 / 360 * ta,
        round(50 / 150 * ta + 100 / 150 * tb),
        round(200 / 302 * tb + 2 / 302 * tc + 100 / 302 * td),
        6 / 6 * ta,
    )
    # In block e circulating supply goes from 858 to 859
    overall_e = round(858 / 859 * overall_d + 1 / 859 * te)
    assert overall_e == 1234560070780
    assert rows[5] == (
        ph + 5,
        overall_e,
        360 / 360 * ta,
        round(50 / 150 * ta + 100 / 150 * tb),
        round(200 / 303 * tb + 2 / 303 * tc + 100 / 303 * td + 1 / 303 * te),
        6 / 6 * ta,
    )

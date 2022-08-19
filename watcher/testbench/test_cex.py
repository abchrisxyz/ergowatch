from os import sync
import pytest
import psycopg as pg

from fixtures.api import MockApi, ApiUtil
from fixtures.scenario.genesis import GENESIS_ID
from fixtures.config import temp_cfg
from fixtures.scenario import Scenario
from fixtures.db import bootstrap_db
from fixtures.db import fill_rev0_db
from fixtures.db import temp_db_class_scoped
from fixtures.db import temp_db_rev0_class_scoped
from fixtures.db import unconstrained_db_class_scoped
from fixtures.scenario.addresses import AddressCatalogue as AC
from test_core import SCENARIO_DESCRIPTION
from utils import run_watcher
from utils import assert_pk
from utils import assert_fk
from utils import assert_unique
from utils import assert_column_not_null
from utils import assert_index
from utils import assert_column_ge

ORDER = 13


SCENARIO_DESCRIPTION = """
    // pub1 is a deposit address for cex1
    // pub2 is a deposit address for cex2
    // pub3 is a deposit address for cex3
    //
    // pub9 appears as a deposit address for cex1 at first
    // but later sends to cex3 too.
    block-a
        // coinbase tx:
        base-box1 1000
        >
        base-box2  840
        con1-box1   60
        pub9-box1  100

    block-b
        // deposit 20 to CEX 1:
        con1-box1   60
        >
        pub1-box1   10
        pub1-box2   10
        con1-box2   40
        --
        // false positive
        // pub9 will be linked to more than 1 cex
        pub9-box1  100
        >
        cex1-box1    6
        pub9-box2   94

    block-c
        // deposit 15 to CEX 2
        con1-box2   40
        >
        pub2-box1   15
        con1-box3   25
        --
        // deposit 5 to CEX 3 (hidden)
        con1-box3   25
        >
        pub3-box1   20
        con1-box4    5
        --
        // cex 1 claiming deposit (deposit was sold)
        pub1-box1   10
        >
        cex1-box2   10

    // ----------------------fork-of-d----------------------
    block-x // fork of block d to be ignored/rolled back:
        -// cex 3 claiming deposit (deposit was sold)
        pub3-box1   20
        >
        cex3-box1   20
        --
        // fake false positive
        // would link pub1 to cex 2 as well
        // to test a conflict rollback
        pub1-box2   10
        >
        cex2-box0   10
    //------------------------------------------------------

    block-d-c
        // cex 2 claiming part of deposit (some deposit was sold)
        pub2-box1   15
        >
        cex2-box1    5
        pub2-box2    9
        fees-box1    1

    //one more block to tell d and x appart and test known deposit addresses
    block-e
        // new cex 2 claim (to test same address is added only once)
        pub2-box2    9
        >
        cex2-box2   3
        pub2-box3   6
        --
        // false positive for deposit addres
        // now linked to a second cex
        // erg still ends up on main though
        pub9-box2   94
        >
        cex3-box2   94
        --
        // contract tx to be ignored
        // con1 will be ignored as deposit address'
        // but supply on cex3 main will increase
        con1-box4    5
        >
        cex3-box3    5
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
        _test_db_state(synced_db, self.scenario, bootstrapped=True)


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
        _test_db_state(synced_db, self.scenario, bootstrapped=True)


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
                    cur.execute("create schema repair;")
                conn.commit()

            # Run
            cp = run_watcher(temp_cfg)
            assert cp.returncode == 0
            assert "Including block block-e" in cp.stdout.decode()
            assert "Repairing 4 blocks (600002 to 600005)" in cp.stdout.decode()
            assert "Done repairing heights 600002 to 600005" in cp.stdout.decode()

            with pg.connect(temp_db_class_scoped) as conn:
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
    the same state as TestGeneis and TestMigrations.
    """
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_cex_ids(cur)
        assert_main_addresses_list(cur)
        assert_ignored_addresses_list(cur)
        assert_main_addresses(cur, s)
        assert_deposit_addresses(cur)
        assert_addresses_conflicts(cur, s)
        assert_processing_log(cur, s, bootstrapped)
        assert_supply(cur, s, bootstrapped)
        assert_repair_cleaned_up(cur)


def assert_db_constraints(conn: pg.Connection):
    # cex.cexs
    assert_pk(conn, "cex", "cexs", ["id"])
    assert_column_not_null(conn, "cex", "cexs", "id")
    assert_column_not_null(conn, "cex", "cexs", "name")
    assert_unique(conn, "cex", "cexs", ["name"])
    # cex.addresses
    assert_pk(conn, "cex", "addresses", ["address_id"])
    assert_fk(conn, "cex", "addresses", "addresses_address_id_fkey")
    assert_fk(conn, "cex", "addresses", "addresses_cex_id_fkey")
    assert_column_not_null(conn, "cex", "addresses", "address_id")
    assert_column_not_null(conn, "cex", "addresses", "cex_id")
    assert_column_not_null(conn, "cex", "addresses", "type")
    assert_index(conn, "cex", "addresses", "addresses_cex_id_idx")
    assert_index(conn, "cex", "addresses", "addresses_type_idx")
    assert_index(conn, "cex", "addresses", "addresses_spot_height_idx")
    # cex.addresses_ignored
    assert_pk(conn, "cex", "addresses_ignored", ["address_id"])
    assert_column_not_null(conn, "cex", "addresses_ignored", "address_id")
    # cex.addresses_conflicts
    assert_pk(conn, "cex", "addresses_conflicts", ["address_id"])
    assert_column_not_null(conn, "cex", "addresses_conflicts", "address_id")
    assert_column_not_null(conn, "cex", "addresses_conflicts", "first_cex_id")
    assert_column_not_null(conn, "cex", "addresses_conflicts", "type")
    assert_fk(
        conn, "cex", "addresses_conflicts", "addresses_conflicts_first_cex_id_fkey"
    )
    # cex.block_processing_log
    assert_pk(conn, "cex", "block_processing_log", ["header_id"])
    assert_column_not_null(conn, "cex", "block_processing_log", "header_id")
    assert_column_not_null(conn, "cex", "block_processing_log", "height")
    assert_column_not_null(conn, "cex", "block_processing_log", "status")
    assert_index(conn, "cex", "block_processing_log", "block_processing_log_status_idx")
    # cex.supply
    assert_pk(conn, "cex", "supply", ["height", "cex_id"])
    assert_column_not_null(conn, "cex", "supply", "height")
    assert_column_not_null(conn, "cex", "supply", "cex_id")
    assert_column_not_null(conn, "cex", "supply", "main")
    assert_column_not_null(conn, "cex", "supply", "deposit")
    assert_fk(conn, "cex", "supply", "supply_cex_id_fkey")
    assert_index(conn, "cex", "supply", "supply_height_idx")
    assert_column_ge(conn, "cex", "supply", "main", 0)
    assert_column_ge(conn, "cex", "supply", "deposit", 0)


def assert_cex_ids(cur: pg.Cursor):
    cur.execute(
        """
        select id
            , name
            , text_id
        from cex.cexs
        order by 1;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 4
    assert rows == [
        (1, "Coinex", "coinex"),
        (2, "Gate.io", "gate"),
        (3, "KuCoin", "kucoin"),
        (4, "ProBit", "probit"),
    ]


def assert_main_addresses_list(cur: pg.Cursor):
    cur.execute(
        """
        select cex_id
            , address
        from cex.main_addresses_list
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 10

    # Coinex
    assert (1, "9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU") in rows
    assert (1, "9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe") in rows
    # Gate
    assert (2, "9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE") in rows
    assert (2, "9gQYrh6yubA4z55u4TtsacKnaEteBEdnY4W2r5BLcFZXcQoQDcq") in rows
    assert (2, "9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC") in rows
    # KuCoin
    assert (3, "9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr") in rows
    assert (3, "9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu") in rows
    assert (3, "9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3") in rows
    assert (3, "9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT") in rows
    # Probit
    assert (4, "9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB") in rows


def assert_ignored_addresses_list(cur: pg.Cursor):
    cur.execute(
        """
        select address
        from cex.ignored_addresses_list;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 4
    assert ("9hxFS2RkmL5Fv5DRZGwZCbsbjTU1R75Luc2t5hkUcR1x3jWzre4",) in rows
    assert ("9gNYeyfRFUipiWZ3JR1ayDMoeh28E6J7aDQosb7yrzsuGSDqzCC",) in rows
    assert ("9i2oKu3bbHDksfiZjbhAgSAWW7iZecUS78SDaB46Fpt2DpUNe6M",) in rows
    assert ("9iHCMtd2gAPoYGhWadjruygKwNKRoeQGq1xjS2Fkm5bT197YFdR",) in rows


def assert_main_addresses(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select c.cex_id
            , a.address
            , c.spot_height
        from cex.addresses c
        join core.addresses a on a.id = c.address_id
        where c.type = 'main'
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 3
    assert (1, s.address("cex1"), s.parent_height + 2) in rows
    assert (2, s.address("cex2"), s.parent_height + 4) in rows
    assert (3, s.address("cex3"), s.parent_height + 5) in rows


def assert_deposit_addresses(cur: pg.Cursor):
    pub1 = AC.get("pub1")
    pub2 = AC.get("pub2")
    cur.execute(
        """
        select c.cex_id
            , a.address
        from cex.addresses c
        join core.addresses a on a.id = c.address_id
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


def assert_addresses_conflicts(cur: pg.Cursor, s: Scenario):
    pub9 = AC.get("pub9")
    cur.execute(
        """
        select a.address
            , c.first_cex_id
            , c.type
            , c.spot_height
            , c.conflict_spot_height
        from cex.addresses_conflicts c
        join core.addresses a on a.id = c.address_id
        order by spot_height;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows == [
        (pub9.address, 1, "deposit", s.parent_height + 2, s.parent_height + 5),
    ]


def assert_processing_log(cur: pg.Cursor, s: Scenario, bootstrapped: bool):
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
    assert rows[0] == (GENESIS_ID, s.parent_height + 0, None, "processed")
    assert rows[1] == ("block-a", s.parent_height + 1, None, expected_status)
    assert rows[2] == ("block-b", s.parent_height + 2, None, expected_status)
    assert rows[3] == (
        "block-c",
        s.parent_height + 3,
        s.parent_height + 2,
        expected_status,
    )
    assert rows[4] == (
        "block-d",
        s.parent_height + 4,
        s.parent_height + 3,
        expected_status,
    )
    assert rows[5] == ("block-e", s.parent_height + 5, None, expected_status)


def assert_supply(cur: pg.Cursor, s: Scenario, bootstrapped: bool):
    height_b = s.parent_height + 2
    height_c = s.parent_height + 3
    height_d = s.parent_height + 4
    height_e = s.parent_height + 5
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
    if bootstrapped:
        assert len(rows) == 6
        assert rows == [
            (height_b, 1, 6, 20),
            (height_c, 1, 16, 10),
            (height_c, 2, 0, 15),
            (height_d, 2, 5, 9),
            (height_e, 2, 8, 6),
            (height_e, 3, 99, 0),
        ]
    else:
        assert len(rows) == 5
        assert rows == [
            (height_b, 1, 6, 94),
            (height_c, 1, 16, 104),
            (height_d, 2, 5, 9),
            (height_e, 2, 8, 6),
            (height_e, 3, 99, 0),
        ]


def assert_repair_cleaned_up(cur: pg.Cursor):
    # Cleanup should have removed remair schema
    cur.execute("create schema repair;")

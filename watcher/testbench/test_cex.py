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
        base-box2  830
        con1-box1   60
        pub8-box1   10
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
        --
        // https://github.com/abchrisxyz/ergowatch/issues/70
        // "airdrop" senders should be ignored as deposit addresses
        pub8-box1   10
        >
        cex1-box8    1
        cex2-box8    1
        pub8-box2    8
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


def _test_db_state(conn: pg.Connection, s: Scenario):
    assert_db_constraints(conn)
    with conn.cursor() as cur:
        assert_cex_ids(cur)
        assert_main_addresses_list(cur)
        assert_ignored_addresses_list(cur)
        assert_main_addresses(cur, s)
        assert_deposit_addresses(cur, s)
        assert_addresses_excluded(cur, s)
        assert_last_processed_height(cur, s)
        assert_supply(cur, s)


def assert_db_constraints(conn: pg.Connection):
    # cex.cexs
    assert_pk(conn, "cex", "cexs", ["id"])
    assert_column_not_null(conn, "cex", "cexs", "id")
    assert_column_not_null(conn, "cex", "cexs", "text_id")
    assert_column_not_null(conn, "cex", "cexs", "name")
    assert_unique(conn, "cex", "cexs", ["text_id"])
    assert_unique(conn, "cex", "cexs", ["name"])
    # cex.main_addresses
    assert_pk(conn, "cex", "main_addresses", ["address_id"])
    assert_fk(conn, "cex", "main_addresses", "main_addresses_address_id_fkey")
    assert_fk(conn, "cex", "main_addresses", "main_addresses_cex_id_fkey")
    assert_column_not_null(conn, "cex", "main_addresses", "address_id")
    assert_column_not_null(conn, "cex", "main_addresses", "cex_id")
    assert_index(conn, "cex", "main_addresses", "main_addresses_cex_id_idx")
    # cex.deposit_addresses
    assert_pk(conn, "cex", "deposit_addresses", ["address_id"])
    assert_fk(conn, "cex", "deposit_addresses", "deposit_addresses_address_id_fkey")
    assert_fk(conn, "cex", "deposit_addresses", "deposit_addresses_cex_id_fkey")
    assert_column_not_null(conn, "cex", "deposit_addresses", "address_id")
    assert_column_not_null(conn, "cex", "deposit_addresses", "cex_id")
    assert_index(conn, "cex", "deposit_addresses", "deposit_addresses_cex_id_idx")
    assert_index(conn, "cex", "deposit_addresses", "deposit_addresses_spot_height_idx")
    # cex.deposit_addresses_ignored
    assert_pk(conn, "cex", "deposit_addresses_ignored", ["address_id"])
    assert_column_not_null(conn, "cex", "deposit_addresses_ignored", "address_id")
    # cex.deposit_addresses_excluded
    assert_pk(conn, "cex", "deposit_addresses_excluded", ["address_id"])
    assert_column_not_null(conn, "cex", "deposit_addresses_excluded", "address_id")
    assert_column_not_null(
        conn, "cex", "deposit_addresses_excluded", "address_spot_height"
    )
    assert_column_not_null(
        conn, "cex", "deposit_addresses_excluded", "conflict_spot_height"
    )

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
    assert len(rows) == 6
    assert rows == [
        (1, "Coinex", "coinex"),
        (2, "Gate.io", "gate"),
        (3, "KuCoin", "kucoin"),
        (4, "ProBit", "probit"),
        (5, "TradeOgre", "tradeogre"),
        (6, "Huobi", "huobi"),
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
    assert len(rows) == 12

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
    # TradeOgre
    assert (5, "9fs99SejQxDjnjwrZ13YMZZ3fwMEVXFewpWWj63nMhZ6zDf2gif") in rows
    # Huobi
    assert (6, "9feMGM1qwNG8NnNuk3pz4yeCGm59s2RbjFnS7DxwUxCbzUrNnJw") in rows


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
        from cex.main_addresses c
        join core.addresses a on a.id = c.address_id
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 3
    assert (1, s.address("cex1")) in rows
    assert (2, s.address("cex2")) in rows
    assert (3, s.address("cex3")) in rows


def assert_deposit_addresses(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select c.cex_id
            , a.address
        from cex.deposit_addresses c
        join core.addresses a on a.id = c.address_id
        order by 1, 2;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 2
    assert rows == [
        (1, s.address("pub1")),
        (2, s.address("pub2")),
    ]


def assert_addresses_excluded(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select a.address
            , c.address_spot_height
            , c.conflict_spot_height
        from cex.deposit_addresses_excluded c
        join core.addresses a on a.id = c.address_id
        order by address_spot_height;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 2
    assert rows == [
        (s.address("pub9"), s.parent_height + 2, s.parent_height + 5),
        (s.address("pub8"), s.parent_height + 5, s.parent_height + 5),
    ]


def assert_last_processed_height(cur: pg.Cursor, s: Scenario):
    cur.execute(
        """
        select last_processed_height
        from cex._deposit_addresses_log;
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0] == (s.parent_height + 5,)


def assert_supply(cur: pg.Cursor, s: Scenario):
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
    for row in rows:
        print(row)
    assert len(rows) == 7
    assert rows == [
        (height_b, 1, 6, 20),
        (height_c, 1, 16, 10),
        (height_c, 2, 0, 15),
        (height_d, 2, 5, 9),
        (height_e, 1, 17, 10),
        (height_e, 2, 9, 6),
        (height_e, 3, 99, 0),
    ]

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
class TestRepairResume:
    """
    Test the -r option to resume a repair.

    Run once to full sync, then inject fake repair session in db
    and rerun with `-r` option.
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

            # Fake interrupted repair session
            with pg.connect(temp_db_class_scoped) as conn:
                conn.execute(
                    """
                    insert into ew.repairs (started, from_height, last_height, next_height)
                    select now(), 600000, 600004, 600002;
                    """
                )

            # Rerun with -r
            cp = run_watcher(temp_cfg, resume_repair=True)
            assert cp.returncode == 0
            assert "Resuming existing repair session" in cp.stdout.decode()
            assert "Done repairing heights 600002 to 600004" in cp.stdout.decode()

    def test_ok(self, synced_db: pg.Connection):
        """
        Dummy test to trigger pytest.
        Actual assertions already passed if we reach this point.
        """
        assert 1

import re
import pytest
import psycopg as pg
import os
from pathlib import Path

from fixtures.api import MockApi, ApiUtil
from fixtures.scenario import Scenario
from fixtures.config import temp_cfg
from fixtures.db import temp_db_rev0
from fixtures.db import SCHEMA_PATH
from fixtures.db import fill_rev0_db
from fixtures.scenario.addresses import AddressCatalogue as AC
from test_mtr_cex import SCENARIO_DESCRIPTION
from utils import run_watcher


# TODO: copied from test_core, but could be simplified a bit
SCENARIO_DESCRIPTION = """
    block-a // coinbase tx
        base-box1 1000
        >
        base-box2  950
        con1-box1   50

    block-b // minting a token and using registers:
        con1-box1   50
        >
        con2-box1   40
        pub1-box1   10 (con1-box1: 2000)

    block-c //using a datainput (in {}) and spending tokens
        pub1-box1   10
        {con2-box1}
        >
        pub1-box2    5 (1500 con1-box1)
        pub2-box1    4 ( 500 con1-box1)
        fees-box1    1
        --
        fees-box1    1
        >
        con1-box2    1
    """


def get_number_of_migrations() -> int:
    """
    Returns number of files in migrations directory
    """
    p = Path(__file__).parent / Path("../src/db/migrations")
    return len(os.listdir(p.resolve()))


@pytest.mark.order(5)
class TestMigrations:

    scenario = Scenario(SCENARIO_DESCRIPTION, 100_000, 1234560000000)

    @pytest.fixture(scope="class")
    def api(self):
        """
        Run watcher with mock api and return cursor to test db.
        """
        with MockApi() as api:
            api = ApiUtil()
            api.set_blocks(self.scenario.blocks)
            yield api

    @pytest.fixture()
    def db(self, api, temp_db_rev0):
        """
        Loads mock api and provides test db connection.

        Function scoped fixture since tests will be changing the db state.
        """
        with pg.connect(temp_db_rev0) as conn:
            # Fill genesis and 1st block only, so we can check if any new blocks got included
            self.scenario.mask(1)
            fill_rev0_db(conn, self.scenario)
            yield conn

    def test_future_db(self, db: pg.Connection, temp_cfg):
        """
        Check watcher stops if DB was created by future version.
        """
        with db.cursor() as cur:
            # Check state before run
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 2
            # Fake migration
            cur.execute("update ew.revision set minor = minor + 1000;")
        db.commit()

        cp = run_watcher(temp_cfg)

        # Check logs
        assert (
            "Database was created by a more recent version of this program"
            in cp.stdout.decode()
        )

        # Check nothing happened
        assert cp.returncode != 0
        with db.cursor() as cur:
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 2

    def test_lagging_db_without_m_option(self, db: pg.Connection, temp_cfg):
        """
        Check watcher stops if DB is behind but migrations are not allowed.
        """
        # Get revision of latest schema
        with open(SCHEMA_PATH) as f:
            major, minor = re.findall(
                r"insert into ew\.revision \(major, minor\) values \((\d+), (\d+)\)",
                f.read(),
            )[0]
            f.read()
            current_rev_major = int(major)
            current_rev_minor = int(minor)

        # Check schema revision matches number of migrations.
        # Migration 1 results in revision 2.1, mig 2 in rev 2.2 and so on...
        assert current_rev_major == 3
        assert current_rev_minor == get_number_of_migrations()

        with db.cursor() as cur:
            # Check state before run
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 2
            # Obtain current db revision
            cur.execute("select major, minor from ew.revision;")
            db_rev_major, db_rev_minor = cur.fetchone()
            assert db_rev_major == current_rev_major
            assert db_rev_minor < current_rev_minor

        cp = run_watcher(temp_cfg)

        # Check logs
        assert (
            f"Database is {current_rev_minor - db_rev_minor} revision(s) behind. Run with the -m option to allow migrations to be applied."
            in cp.stdout.decode()
        )

        # Check nothing happened
        assert cp.returncode != 0
        with db.cursor() as cur:
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 2

    def test_migrations_are_applied_if_allowed(self, db: pg.Connection, temp_cfg):
        """
        Check migrations are applied and watcher proceeds normally.
        """
        # Check db is at version 1.0 initially
        with db.cursor() as cur:
            cur.execute("select major, minor from ew.revision;")
            assert cur.fetchone() == (3, 0)

        cp = run_watcher(temp_cfg, allow_migrations=True)

        # Check logs
        assert cp.returncode == 0
        rev = get_number_of_migrations()
        assert f"Applying migration {rev}" in cp.stdout.decode()

        # Check migrations are applied
        with db.cursor() as cur:
            cur.execute("select major, minor from ew.revision;")
            assert cur.fetchone() == (3, rev)

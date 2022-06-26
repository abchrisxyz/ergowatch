import re
import pytest
import psycopg as pg
import os
from pathlib import Path

from fixtures.api import MockApi, ApiUtil
from fixtures.scenario import Scenario
from fixtures.config import temp_cfg
from fixtures.db import temp_db_rev1
from fixtures.db import SCHEMA_PATH
from fixtures.db import fill_rev1_db
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


@pytest.mark.skip("TODO")
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
    def db(self, api, temp_db_rev1):
        """
        Loads mock api and provides test db connection.

        Function scoped fixture since tests will be changing the db state.
        """
        with pg.connect(temp_db_rev1) as conn:
            # Fill genesis and 1st block only, so we can check if any new blocks got included
            self.scenario.mask(1)
            fill_rev1_db(conn, self.scenario)
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
            cur.execute("update ew.revision set version = version + 1000;")
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
            current_revision = re.findall(
                r"insert into ew\.revision \(version\) values \((\d+)\)", f.read()
            )[0]
            f.read()
            current_revision = int(current_revision)

        # Check schema revision matches number of migrations.
        # Migration 1 results in revision 2, and so on...
        assert current_revision == get_number_of_migrations() + 1

        with db.cursor() as cur:
            # Check state before run
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 2
            # Obtain current db revision
            cur.execute("select version from ew.revision;")
            db_revision = cur.fetchone()[0]
            assert db_revision < current_revision
            assert db_revision == 1

        cp = run_watcher(temp_cfg)

        # Check logs
        assert (
            f"Database is {current_revision - db_revision} revision(s) behind. Run with the -m option to allow migrations to be applied."
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
        # Check db is at version 1 initially
        with db.cursor() as cur:
            cur.execute("select version from ew.revision;")
            assert cur.fetchone()[0] == 1

        cp = run_watcher(temp_cfg, allow_migrations=True)

        # Check logs
        assert cp.returncode == 0
        n_migs = get_number_of_migrations()
        rev = n_migs + 1
        assert f"Applying migration {n_migs} (revision {rev})" in cp.stdout.decode()

        # Check migrations are applied
        with db.cursor() as cur:
            cur.execute("select version from ew.revision;")
            assert cur.fetchone()[0] == rev

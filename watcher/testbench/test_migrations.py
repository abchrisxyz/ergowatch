import pytest

from fixtures import genesis_env
from utils import run_watcher


@pytest.mark.order(2)
class TestMigrations:
    def test_future_db(self, genesis_env):
        """
        Check watcher stops if DB was created by future version.
        """
        db_conn, cfg_path = genesis_env

        with db_conn.cursor() as cur:
            cur.execute("update ew.revision set version = version + 1;")
        db_conn.commit()

        cp = run_watcher(cfg_path)

        # Check logs
        assert (
            "Database was created by a more recent version of this program"
            in cp.stdout.decode()
        )

        # Check nothing happened
        assert cp.returncode != 0
        with db_conn.cursor() as cur:
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 0

    def test_lagging_db_without_allowing_migrations(self, genesis_env):
        """
        Check watcher stops if DB is behind but migrations are not allowed.
        """
        db_conn, cfg_path = genesis_env

        with db_conn.cursor() as cur:
            cur.execute("update ew.revision set version = version - 1;")
        db_conn.commit()

        cp = run_watcher(cfg_path)

        # Check logs
        assert (
            "Database is 1 revision(s) behind. Run with the -m option to allow migrations to be applied."
            in cp.stdout.decode()
        )

        # Check nothing happened
        assert cp.returncode != 0
        with db_conn.cursor() as cur:
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 0

    @pytest.mark.skip("No migrations to test yet")
    def test_migration_are_applied_if_allowed(self, genesis_env):
        """
        Check migrations are applied and watcher proceeds normally.
        """
        db_conn, cfg_path = genesis_env

        # Check db is at version 1 initially
        with db_conn.cursor() as cur:
            cur.execute("select version from ew.revision;")
            assert cur.fetchone[0] == 1

        cp = run_watcher(cfg_path)

        # Check logs
        assert "Applying migration " in cp.stdout.decode()

        # Check db was synced
        assert cp.returncode == 0
        with db_conn.cursor() as cur:
            cur.execute("select count(*) from core.headers;")
            assert cur.fetchone()[0] == 1
            # And migration was applied
            cur.execute("select version from ew.revision;")
            assert cur.fetchone()[0] == 2

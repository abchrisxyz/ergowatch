import psycopg as pg
from fixtures import balances_env
from fixtures import balances_bootstrap_env
from utils import run_watcher
from utils import extract_db_conn_str


class TestNormalMode:
    """
    Test stuff in normal mode (i.e. no bootstrapping).
    """

    def test_erg_diffs(self, balances_env):
        """
        Check Erg balance changes are recorded correctly.
        """
        db_conn, cfg_path = balances_env

        # Check db state berofe run
        with db_conn.cursor() as cur:
            cur.execute(
                "select creation_height, address, value from core.outputs order by 1, 2, 3;"
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (
            619220,
            "dummy-address-0",
            93000000000000000,
        )

        # Run
        cp = run_watcher(cfg_path)
        assert cp.returncode == 0

        # Check db state after run
        with db_conn.cursor() as cur:
            cur.execute(
                "select address, height, tx_id, value from bal.erg_diffs order by 2, 1, 3;"
            )
            rows = cur.fetchall()
            assert len(rows) == 6

            assert rows[0] == (
                "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
                619221,
                "a41d918241b648a1ee3e984c82d8aebbac0b344a9e702ca85e89742fd82e718e",
                1000000,
            )

            assert rows[1] == (
                "9gznmZubB3LFxoEQFtQ8njvjdmjUnH2w1pvb4yCtgv6HXYfBF3Q",
                619221,
                "a41d918241b648a1ee3e984c82d8aebbac0b344a9e702ca85e89742fd82e718e",
                392600000,  # would be 392600000 - 393750000 if db mock would contain the input,
            )

            assert rows[2] == (
                "9hbQaRBqhVsC1kxckwX19Rbo3pXfFwT4Zv2z8Ytbnc8qTooZWYo",
                619221,
                "a41d918241b648a1ee3e984c82d8aebbac0b344a9e702ca85e89742fd82e718e",
                150000,
            )

            assert rows[3] == (
                "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
                619222,
                "abc1dfbc096e0ae2017eda4a5cd3c9a4c585855f16f9274462b66fec015d327d",
                1000000,
            )

            assert rows[4] == (
                "9gznmZubB3LFxoEQFtQ8njvjdmjUnH2w1pvb4yCtgv6HXYfBF3Q",
                619222,
                "abc1dfbc096e0ae2017eda4a5cd3c9a4c585855f16f9274462b66fec015d327d",
                391450000 - 392600000,
            )

            assert rows[5] == (
                "9i8KzTfqokBWXdqS9DVSWmRFHR4DyJ5FMiCgxNGqCoMb31xA7Y5",
                619222,
                "abc1dfbc096e0ae2017eda4a5cd3c9a4c585855f16f9274462b66fec015d327d",
                150000,
            )

    def test_erg_balances(self, balances_env):
        """
        Check Erg balances are set correctly.
        """
        db_conn, cfg_path = balances_env

        # Check db state berofe run
        with db_conn.cursor() as cur:
            cur.execute(
                "select creation_height, address, value from core.outputs order by 1, 2, 3;"
            )
            rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0] == (
            619220,
            "dummy-address-0",
            93000000000000000,
        )

        # Run
        cp = run_watcher(cfg_path)
        assert cp.returncode == 0

        # Check db state after run
        with db_conn.cursor() as cur:
            cur.execute("select address, value from bal.erg order by 1;")
            rows = cur.fetchall()
            assert len(rows) == 5

            assert rows[0] == (
                "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
                1000000 + 1000000,
            )

            assert rows[1] == (
                "9gznmZubB3LFxoEQFtQ8njvjdmjUnH2w1pvb4yCtgv6HXYfBF3Q",
                392600000 - 1150000,
            )

            assert rows[2] == (
                "9hbQaRBqhVsC1kxckwX19Rbo3pXfFwT4Zv2z8Ytbnc8qTooZWYo",
                150000,
            )

            assert rows[3] == (
                "9i8KzTfqokBWXdqS9DVSWmRFHR4DyJ5FMiCgxNGqCoMb31xA7Y5",
                150000,
            )

            # This one was already present in the test db
            assert rows[4] == (
                "dummy-address-0",
                93000000000000000,
            )


class TestBootstrapMode:
    """
    Test stuff in bootstrap mode.
    """

    def test_erg_diffs(self, balances_bootstrap_env):
        """
        Check Erg balance changes are bootstrapped correctly.
        """
        db_conn, cfg_path = balances_bootstrap_env

        # Close db conn to avoid relation locks when constraints get set.
        # Keep the connection string at hand so we can reconnect to inspect.
        conn_str = extract_db_conn_str(db_conn)
        db_conn.close()

        # Check db state berofe run
        with pg.connect(conn_str) as db_conn:
            with db_conn.cursor() as cur:
                cur.execute(
                    "select creation_height, address, value from core.outputs order by 1, 2, 3;"
                )
                rows = cur.fetchall()
            assert len(rows) == 1
            assert rows[0] == (
                619220,
                "dummy-address-0",
                93000000000000000,
            )

        # Run
        cp = run_watcher(cfg_path, bootstrap=True, sync_only=False)
        assert cp.returncode == 0

        # Check db state after run
        with pg.connect(conn_str) as db_conn:
            with db_conn.cursor() as cur:
                cur.execute(
                    "select address, height, tx_id, value from bal.erg_diffs order by 2, 1, 3;"
                )
                rows = cur.fetchall()
                assert len(rows) == 7
                assert rows[0][1] == 619220

                # This one is from the db init sql (see db.py::generate_bootstrap_sql)
                assert rows[0] == (
                    "dummy-address-0",
                    619220,
                    "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
                    93000000000000000,
                )

                assert rows[1] == (
                    "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
                    619221,
                    "a41d918241b648a1ee3e984c82d8aebbac0b344a9e702ca85e89742fd82e718e",
                    1000000,
                )

                assert rows[2] == (
                    "9gznmZubB3LFxoEQFtQ8njvjdmjUnH2w1pvb4yCtgv6HXYfBF3Q",
                    619221,
                    "a41d918241b648a1ee3e984c82d8aebbac0b344a9e702ca85e89742fd82e718e",
                    392600000,  # would be 392600000 - 393750000 if db mock would contain the input,
                )

                assert rows[3] == (
                    "9hbQaRBqhVsC1kxckwX19Rbo3pXfFwT4Zv2z8Ytbnc8qTooZWYo",
                    619221,
                    "a41d918241b648a1ee3e984c82d8aebbac0b344a9e702ca85e89742fd82e718e",
                    150000,
                )

                assert rows[4] == (
                    "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
                    619222,
                    "abc1dfbc096e0ae2017eda4a5cd3c9a4c585855f16f9274462b66fec015d327d",
                    1000000,
                )

                assert rows[5] == (
                    "9gznmZubB3LFxoEQFtQ8njvjdmjUnH2w1pvb4yCtgv6HXYfBF3Q",
                    619222,
                    "abc1dfbc096e0ae2017eda4a5cd3c9a4c585855f16f9274462b66fec015d327d",
                    391450000 - 392600000,
                )

                assert rows[6] == (
                    "9i8KzTfqokBWXdqS9DVSWmRFHR4DyJ5FMiCgxNGqCoMb31xA7Y5",
                    619222,
                    "abc1dfbc096e0ae2017eda4a5cd3c9a4c585855f16f9274462b66fec015d327d",
                    150000,
                )

    def test_erg_balances(self, balances_bootstrap_env):
        """
        Check Erg balances are set correctly.
        """
        db_conn, cfg_path = balances_bootstrap_env

        # Close db conn to avoid relation locks when constraints get set.
        # Keep the connection string at hand so we can reconnect to inspect.
        conn_str = extract_db_conn_str(db_conn)
        db_conn.close()

        # Check db state berofe run
        with pg.connect(conn_str) as db_conn:
            with db_conn.cursor() as cur:
                cur.execute(
                    "select creation_height, address, value from core.outputs order by 1, 2, 3;"
                )
                rows = cur.fetchall()
            assert len(rows) == 1
            assert rows[0] == (
                619220,
                "dummy-address-0",
                93000000000000000,
            )

        # Run
        cp = run_watcher(cfg_path, bootstrap=True, sync_only=False)
        assert cp.returncode == 0

        # Check db state after run
        with pg.connect(conn_str) as db_conn:
            with db_conn.cursor() as cur:
                cur.execute("select address, value from bal.erg order by 1;")
                rows = cur.fetchall()
                assert len(rows) == 5

                assert rows[0] == (
                    "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe",
                    1000000 + 1000000,
                )

                assert rows[1] == (
                    "9gznmZubB3LFxoEQFtQ8njvjdmjUnH2w1pvb4yCtgv6HXYfBF3Q",
                    392600000 - 1150000,
                )

                assert rows[2] == (
                    "9hbQaRBqhVsC1kxckwX19Rbo3pXfFwT4Zv2z8Ytbnc8qTooZWYo",
                    150000,
                )

                assert rows[3] == (
                    "9i8KzTfqokBWXdqS9DVSWmRFHR4DyJ5FMiCgxNGqCoMb31xA7Y5",
                    150000,
                )

                assert rows[4] == (
                    "dummy-address-0",
                    93000000000000000,
                )

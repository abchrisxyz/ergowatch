import psycopg as pg
from fixtures import unspent_env
from fixtures import unspent_bootstrap_env
from utils import run_watcher
from utils import extract_db_conn_str


class TestNormalMode:
    """
    Test stuff in normal mode (i.e. no bootstrapping).
    """

    def test_boxes(self, unspent_env):
        """
        Check unspent boxes are tracked correctly.
        """
        db_conn, cfg_path = unspent_env

        # Check db state berofe run
        with db_conn.cursor() as cur:
            cur.execute("select count(*) from usp.boxes;")
            assert cur.fetchone()[0] == 4

        # Run
        cp = run_watcher(cfg_path)
        assert cp.returncode == 0

        # Check db state after run
        with db_conn.cursor() as cur:
            cur.execute("select box_id from usp.boxes order by 1;")
            rows = cur.fetchall()
            assert len(rows) == 6
            assert rows[0] == (
                "029bc1cb151aaef51c3678d2c74f3e82c9f4d197dd37e7a4eb73612f9da4f1f6",
            )
            assert rows[1] == (
                "22adc6d1fd18e81da0ab9fa47bc389c5948780c98906c0ea3d812eba4ef17a33",
            )
            assert rows[2] == (
                "5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4",
            )
            assert rows[3] == (
                "6cb8ffe391838b627cb893c9b2027aa2a03f3a20455dd11e5ac903c7e4179ace",
            )
            assert rows[4] == (
                "98d0271b7a29d62b672d8dd002e38b8cfbfc8e4055a637422b3e9d59cd6ff86d",
            )
            assert rows[5] == (
                "aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d",
            )


class TestBootstrapMode:
    """
    Test stuff in bootstrap mode.
    """

    def test_boxes(self, unspent_bootstrap_env):
        """
        Check Erg balance changes are bootstrapped correctly.
        """
        db_conn, cfg_path = unspent_bootstrap_env

        # Close db conn to avoid relation locks when constraints get set.
        # Keep the connection string at hand so we can reconnect to inspect.
        conn_str = extract_db_conn_str(db_conn)
        db_conn.close()

        # Run
        cp = run_watcher(cfg_path, bootstrap=True, sync_only=False)
        assert cp.returncode == 0

        # Check db state after run
        with pg.connect(conn_str) as db_conn:
            # Check db state after run
            # Should contain 5 unspent outputs from block 600k + 1 from the mock db.
            #
            # Block 600k generates 6 outputs, but
            #   5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4 is
            # is also spent within block 600k, so not expected here.
            #
            # The mock db init sql produces 2 outputs:
            #   eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f
            # which gets spend in block 600k, and
            #   98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8
            # which is there to satisfy the FK of a data-input present in block 600k
            #
            # Genesis boxes should've been spent in reality, but mock db doesn't
            # contain all txs, so they might be unspent here. Howeber, since we're not starting
            # with an empty db, the genesis boxes are not included at all in the db.
            # Mock DB init sql output id:
            #   eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f
            with db_conn.cursor() as cur:
                cur.execute("select box_id from usp.boxes order by 1;")
                rows = cur.fetchall()
                assert len(rows) == 5 + 1
                print(rows)
                assert rows[0] == (
                    "029bc1cb151aaef51c3678d2c74f3e82c9f4d197dd37e7a4eb73612f9da4f1f6",
                )
                assert rows[1] == (
                    "22adc6d1fd18e81da0ab9fa47bc389c5948780c98906c0ea3d812eba4ef17a33",
                )
                assert rows[2] == (
                    "6cb8ffe391838b627cb893c9b2027aa2a03f3a20455dd11e5ac903c7e4179ace",
                )
                assert rows[3] == (
                    "98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8",  # inserted datainput
                )
                assert rows[4] == (
                    "98d0271b7a29d62b672d8dd002e38b8cfbfc8e4055a637422b3e9d59cd6ff86d",
                )
                assert rows[5] == (
                    "aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d",
                )

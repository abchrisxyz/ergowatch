import psycopg as pg
from fixtures import balances_bootstrap_env
from utils import run_watcher
from utils import extract_db_conn_str


def test_erg_diffs(balances_bootstrap_env):
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
            "2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU",
            93409065000000000,
        )

    # Run
    cp = run_watcher(cfg_path, bootstrap=True, sync_only=False, timeout=120)
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
                "2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU",
                619220,
                "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
                93409065000000000,
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

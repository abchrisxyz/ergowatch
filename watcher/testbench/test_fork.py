import requests

from fixtures import fork_env
from fixtures import unconstrained_db_env
from utils import run_watcher


def test_side_chain_is_ignored(fork_env):
    """
    Test watcher picks main_chain block
    """
    db_conn, cfg_path = fork_env
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    # Read db to verify state
    with db_conn.cursor() as cur:
        cur.execute("select height from core.headers order by 1 desc limit 1;")
        assert cur.fetchone()[0] == 672_221

        # Check headers
        cur.execute("select height, id from core.headers order by 1;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [672_219, 672_220, 672_221]
        # Header id's
        assert [r[1] for r in rows] == [
            "63be0d9eb0ed2bb466898b0a11d73bdab5d645b1f289e5f9c2304d966ae7a2f5",
            "8487755bf634497f4693f43e36ffc43dc0b1db9c0a317dd2952a94e7d5aa61e9",
            "4d47f7871609a26a7f18a4591d14c2098dbeda4e278c90da11167152a90ea694",
        ]

        # Check transactions
        cur.execute("select height, id from core.transactions order by height, index;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [672_219, 672_220, 672_221, 672_221]
        # Transaction id's
        assert [r[1] for r in rows] == [
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
            "6b3eb09041bf1759f300c3a1209406504c3b301eb7a445d7cfe719b0bca6132c",
            "546591655290f20359637468d275c822d6fd6ba784e65e58b6e6914a6ebe929a",
            "e211bf9d7a070d0c2b65031426404f3a62aee45a48b3f7cea541060344a1513a",
        ]


# normal: 6b3eb09041bf1759f300c3a1209406504c3b301eb7a445d7cfe719b0bca6132c
# fork: cd536290bc4ea4e63af9c4da19b60f7dcbaadb59570dfc7e659f45759f5ec15f
# next:  546591655290f20359637468d275c822d6fd6ba784e65e58b6e6914a6ebe929a
#        e211bf9d7a070d0c2b65031426404f3a62aee45a48b3f7cea541060344a1513a


def test_forked_chain_is_rolled_back(fork_env):
    """
    Test watcher rolls back forked blocks
    """
    db_conn, cfg_path = fork_env

    r = requests.get(f"http://localhost:9053/enable_stepping")
    assert r.status_code == 200

    # First run. This will include block 672220_fork
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    with db_conn.cursor() as cur:
        # Check headers
        cur.execute("select height, id from core.headers order by 1;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [672_219, 672_220]
        # Header id's
        assert [r[1] for r in rows] == [
            "63be0d9eb0ed2bb466898b0a11d73bdab5d645b1f289e5f9c2304d966ae7a2f5",
            "6c48253ece1c7a7e832ef37f9366448f43f47ec0d16f86d91b3fab48ac53816a",
        ]

        # Check transactions
        cur.execute("select height, id from core.transactions order by height, index;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [672_219, 672_220]
        # Transaction id's
        assert [r[1] for r in rows] == [
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
            "cd536290bc4ea4e63af9c4da19b60f7dcbaadb59570dfc7e659f45759f5ec15f",
        ]

    # Step
    r = requests.get(f"http://localhost:9053/step")
    assert r.status_code == 200

    # Second run. Should change nothing as node is still at height 672220.
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    with db_conn.cursor() as cur:
        # Check headers
        cur.execute("select height, id from core.headers order by 1;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [672_219, 672_220]
        # Header id's
        assert [r[1] for r in rows] == [
            "63be0d9eb0ed2bb466898b0a11d73bdab5d645b1f289e5f9c2304d966ae7a2f5",
            "6c48253ece1c7a7e832ef37f9366448f43f47ec0d16f86d91b3fab48ac53816a",
        ]

        # Check transactions
        cur.execute("select height, id from core.transactions order by height, index;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [672_219, 672_220]
        # Transaction id's
        assert [r[1] for r in rows] == [
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
            "cd536290bc4ea4e63af9c4da19b60f7dcbaadb59570dfc7e659f45759f5ec15f",
        ]

    # Step
    r = requests.get(f"http://localhost:9053/step")
    assert r.status_code == 200

    # Third run. Should roll back 672220_fork, include 672220 instead, and then 672221.
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    with db_conn.cursor() as cur:
        # Check heights
        cur.execute("select height, id from core.headers order by 1;")
        rows = cur.fetchall()
        # Heights
        assert [r[0] for r in rows] == [672_219, 672_220, 672_221]
        # Header id's
        assert [r[1] for r in rows] == [
            "63be0d9eb0ed2bb466898b0a11d73bdab5d645b1f289e5f9c2304d966ae7a2f5",
            "8487755bf634497f4693f43e36ffc43dc0b1db9c0a317dd2952a94e7d5aa61e9",
            "4d47f7871609a26a7f18a4591d14c2098dbeda4e278c90da11167152a90ea694",
        ]

        # Check transactions
        cur.execute("select height, id from core.transactions order by height, index;")
        rows = cur.fetchall()
        # Heights
        print([r[0] for r in rows])
        assert [r[0] for r in rows] == [672_219, 672_220, 672_221, 672_221]
        # Transaction id's
        assert [r[1] for r in rows] == [
            "4c6282be413c6e300a530618b37790be5f286ded758accc2aebd41554a1be308",
            "6b3eb09041bf1759f300c3a1209406504c3b301eb7a445d7cfe719b0bca6132c",
            "546591655290f20359637468d275c822d6fd6ba784e65e58b6e6914a6ebe929a",
            "e211bf9d7a070d0c2b65031426404f3a62aee45a48b3f7cea541060344a1513a",
        ]


def test_watcher_exits_on_unconstrained_db_without_sync_only_option(
    unconstrained_db_env,
):
    """
    Rollbacks rely partly on foreign keys to propagate through the db.

    The watcher should only be run without constraints when using the -s option,
    which guarantees there will be no rollbacks.

    This test checks the watcher exits when omitting the -s option
    with an unconstrained db.
    """
    db_conn, cfg_path = unconstrained_db_env
    cp = run_watcher(cfg_path, sync_only=False)
    assert cp.returncode != 0

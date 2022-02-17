import requests

from fixtures import block_600k_env
from fixtures import fork_env
from fixtures import unconstrained_db_env
from utils import run_watcher


def test_constraints_are_set_after_bootstrapping(unconstrained_db_env):
    """
    When done syncing, bootstrapping mode should load database constraints.
    """
    db_conn, cfg_path = unconstrained_db_env

    # No constraints before run
    with db_conn.cursor() as cur:
        cur.execute("select constraints_set from ew.revision;")
        row = cur.fetchone()
    assert row[0] == False

    # Run
    cp = run_watcher(cfg_path, bootstrap=True, sync_only=False)
    assert cp.returncode == 0

    # Constrains after run
    with db_conn.cursor() as cur:
        cur.execute("select constraints_set from ew.revision;")
        row = cur.fetchone()
    assert row[0] == True


def test_constraints_set_constraints_flag(fork_env):
    """
    Test db relevant db flag is set after applying constraints.
    """
    db_conn, cfg_path = fork_env
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0


def test_bootsrap_mode_is_prevented_on_constrained_db(
    block_600k_env,
):
    """
    Make sure bootstrap mode is not allowed on a constrained db
    """
    db_conn, cfg_path = block_600k_env
    cp = run_watcher(cfg_path, bootstrap=True, sync_only=False)
    assert cp.returncode != 0

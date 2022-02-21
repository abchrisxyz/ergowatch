import pytest

from fixtures import block_600k_env
from fixtures import unconstrained_db_env
from fixtures import bootstrap_empty_db_env
from utils import run_watcher


@pytest.mark.order(2)
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


@pytest.mark.order(2)
def test_constraints_set_constraints_flag(block_600k_env):
    """
    Test the relevant db flag is set after applying constraints.
    """
    db_conn, cfg_path = block_600k_env
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0


@pytest.mark.order(2)
def test_bootsrap_mode_is_prevented_on_constrained_db(
    block_600k_env,
):
    """
    Make sure bootstrap mode is not allowed on a constrained db
    """
    db_conn, cfg_path = block_600k_env
    cp = run_watcher(cfg_path, bootstrap=True, sync_only=False)
    assert cp.returncode != 0


@pytest.mark.order(2)
def test_watcher_exits_on_unconstrained_db_without_bootstrap_option(
    unconstrained_db_env,
):
    """
    Rollbacks rely partly on foreign keys to propagate through the db.

    The watcher should only be run without constraints when using the -b option.

    Checks the watcher exits when omitting the -b option with an unconstrained db.
    """
    db_conn, cfg_path = unconstrained_db_env
    cp = run_watcher(cfg_path)
    assert cp.returncode != 0


@pytest.mark.order(2)
def test_watcher_exits_with_both_bootstrap_and_sync_only_options(
    unconstrained_db_env,
):
    """
    Options -b and -s should not be used together
    """
    db_conn, cfg_path = unconstrained_db_env
    cp = run_watcher(cfg_path, bootstrap=True)
    assert cp.returncode != 0

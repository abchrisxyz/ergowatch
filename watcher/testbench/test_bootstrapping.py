import requests

from fixtures import block_600k_env
from fixtures import fork_env
from fixtures import unconstrained_db_env
from utils import run_watcher


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

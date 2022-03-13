import pytest

from fixtures import unconstrained_db_env
from utils import run_watcher


@pytest.mark.order(3)
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
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    # Constraints after run
    with db_conn.cursor() as cur:
        cur.execute("select constraints_set from ew.revision;")
        row = cur.fetchone()
    assert row[0] == True

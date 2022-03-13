import pytest

from fixtures import block_600k_env
from utils import run_watcher


@pytest.mark.order(2)
def test_constraints_set_constraints_flag(block_600k_env):
    """
    Test the relevant db flag is set after applying constraints.
    """
    # block_600k_env has a db with contraints loaded already
    db_conn, cfg_path = block_600k_env
    with db_conn.cursor() as cur:
        cur.execute("select constraints_set from ew.revision;")
        row = cur.fetchone()
    assert row[0] == True

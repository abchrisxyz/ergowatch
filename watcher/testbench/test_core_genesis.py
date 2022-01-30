from fixtures import genesis_env
from utils import run_watcher


def test_first_block(genesis_env):
    """
    Check connection works and db is blank
    """
    db_conn, cfg_path = genesis_env
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    # Read db to verify state
    with db_conn.cursor() as cur:
        cur.execute("select height from core.headers order by 1 desc limit 1;")
        assert cur.fetchone()[0] == 1
        cur.execute("select count(*) from core.transactions;")
        assert cur.fetchone()[0] == 1
        cur.execute("select count(*) from core.outputs;")
        assert cur.fetchone()[0] == 2
        cur.execute("select count(*) from core.inputs;")
        assert cur.fetchone()[0] == 1

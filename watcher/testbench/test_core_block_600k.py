from fixtures import block_600k_env
from utils import run_watcher


def test_block_600k(block_600k_env):
    """
    Test db state after including block 600k
    """
    db_conn, cfg_path = block_600k_env

    # DB state before run
    with db_conn.cursor() as cur:
        cur.execute("select height from core.headers order by 1 desc limit 1;")
        assert cur.fetchone()[0] == 599_999
        cur.execute("select address, value from core.outputs order by 1;")
        rows = cur.fetchall()
        assert len(rows) == 2
        assert rows == [
            ("dummy-address-0", 93000000000000000),
            ("dummy-address-1", 67500000000),
        ]

    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    # Read db to verify state
    with db_conn.cursor() as cur:
        cur.execute("select height from core.headers order by 1 desc limit 1;")
        assert cur.fetchone()[0] == 600_000
        cur.execute("select count(*) from core.transactions;")
        assert cur.fetchone()[0] == 1 + 3
        cur.execute("select count(*) from core.outputs;")
        assert cur.fetchone()[0] == 2 + 6
        cur.execute("select count(*) from core.inputs;")
        assert cur.fetchone()[0] == 4
        cur.execute("select count(*) from core.data_inputs;")
        assert cur.fetchone()[0] == 1
        cur.execute("select count(*) from core.box_registers;")
        assert cur.fetchone()[0] == 3
        cur.execute("select count(*) from core.tokens;")
        assert cur.fetchone()[0] == 1 + 0
        cur.execute("select count(*) from core.box_assets;")
        assert cur.fetchone()[0] == 1

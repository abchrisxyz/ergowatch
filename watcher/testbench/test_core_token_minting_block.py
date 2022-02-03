from fixtures import token_minting_env
from utils import run_watcher


def test_row_counts(token_minting_env):
    """
    Test db state after including block 600k
    """
    db_conn, cfg_path = token_minting_env
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    # Read db to verify state
    with db_conn.cursor() as cur:
        cur.execute("select height from core.headers order by 1 desc limit 1;")
        assert cur.fetchone()[0] == 600_001
        cur.execute("select count(*) from core.transactions;")
        assert cur.fetchone()[0] == 1 + 2
        cur.execute("select count(*) from core.outputs;")
        assert cur.fetchone()[0] == 1 + 6
        cur.execute("select count(*) from core.inputs;")
        assert cur.fetchone()[0] == 3
        cur.execute("select count(*) from core.data_inputs;")
        assert cur.fetchone()[0] == 0
        cur.execute("select count(*) from core.box_registers;")
        assert cur.fetchone()[0] == 6
        cur.execute("select count(*) from core.tokens;")
        assert cur.fetchone()[0] == 0 + 2
        cur.execute("select count(*) from core.box_assets;")
        assert cur.fetchone()[0] == 2


def test_token_data_eip4(token_minting_env):
    """
    Test db state after including block 600k
    """
    db_conn, cfg_path = token_minting_env
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    # Read db to verify state
    with db_conn.cursor() as cur:
        cur.execute(
            "select * from core.tokens where id = '34d14f73cc1d5342fb06bc1185bd1335e8119c90b1795117e2874ca6ca8dd2c5';"
        )
        rec = cur.fetchone()
    assert rec[0] == "34d14f73cc1d5342fb06bc1185bd1335e8119c90b1795117e2874ca6ca8dd2c5"
    assert rec[1] == "5410f440002d0f350781463633ff6be869c54149cebeaeb935eb2968918e846b"
    assert rec[2] == 5000
    assert rec[3] == "best"
    assert rec[4] == "test "
    assert rec[5] == 1
    assert rec[6] == "EIP-004"


def test_token_data_generic(token_minting_env):
    """
    Test db state after including block 600k
    """
    db_conn, cfg_path = token_minting_env
    cp = run_watcher(cfg_path)
    assert cp.returncode == 0

    # Read db to verify state
    with db_conn.cursor() as cur:
        cur.execute(
            "select * from core.tokens where id = '3c65b325ebf58f4907d6c085d216e176d105a5093540704baf1f7a2a42ad60f8';"
        )
        rec = cur.fetchone()
    assert rec[0] == "3c65b325ebf58f4907d6c085d216e176d105a5093540704baf1f7a2a42ad60f8"
    assert rec[1] == "48461e901b2a518d66b8d147a5282119cfc5b065a3ebba6a56b354686686a48c"
    assert rec[2] == 1000
    assert rec[3] is None
    assert rec[4] is None
    assert rec[5] is None
    assert rec[6] is None

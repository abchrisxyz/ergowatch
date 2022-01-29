from asyncio.subprocess import STDOUT
import subprocess
from pathlib import Path

from fixtures import genesis_env


def test_first_block(genesis_env):
    """
    Check connection works and db is blank
    """
    db_conn, cfg_path = genesis_env
    exe = str(Path(__file__).parent.parent.absolute() / Path("target/release/watcher"))
    cp = subprocess.run(
        [exe, "-c", cfg_path, "--sync-only"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={
            "RUST_LOG": "INFO",
        },
        timeout=10,
    )
    assert cp.stderr is None
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

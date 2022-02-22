import os
from pathlib import Path
import subprocess


def run_watcher(
    cfg_path: Path,
    target="release",
    sync_only=True,
    bootstrap=False,
    backtrace=False,
    timeout=10,
) -> subprocess.CompletedProcess:
    exe = str(
        Path(__file__).parent.parent.absolute() / Path(f"target/{target}/watcher")
    )
    args = [exe, "-c", cfg_path]
    if sync_only:
        args.append("--sync-once")
    if bootstrap:
        args.append("--bootstrap")

    # Path to constraints definitions
    sql = Path(__file__).parent.parent.absolute() / Path(f"db/constraints.sql")
    args.extend(["-k", str(sql)])

    env = dict(
        os.environ,
        EW_LOG="DEBUG",
    )
    if backtrace:
        env["RUST_BACKTRACE"] = "full"

    print(args)
    cp = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        timeout=timeout,
    )
    print(cp.stdout.decode())
    return cp


def extract_db_conn_str(db_conn) -> str:
    db_info = db_conn.info
    return f"host={db_info.host} port={db_info.port} dbname={db_info.dbname} user={db_info.user} password={db_info.password}"

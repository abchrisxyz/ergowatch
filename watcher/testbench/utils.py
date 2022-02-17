import os
from pathlib import Path
import subprocess


def run_watcher(
    cfg_path: Path, target="release", sync_only=True, bootstrap=False, backtrace=False
) -> subprocess.CompletedProcess:
    exe = str(
        Path(__file__).parent.parent.absolute() / Path(f"target/{target}/watcher")
    )
    args = [exe, "-c", cfg_path]
    if sync_only:
        args.append("--sync-once")
    if bootstrap:
        sql = Path(__file__).parent.parent.absolute() / Path(f"db/constraints.sql")
        args.append("--bootstrap")
        args.extend(["-k", str(sql)])

    env = dict(
        os.environ,
        EW_LOG="DEBUG",
    )
    if backtrace:
        env["RUST_BACKTRACE"] = "full"

    cp = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        timeout=10,
    )
    print(cp.stdout.decode())
    return cp

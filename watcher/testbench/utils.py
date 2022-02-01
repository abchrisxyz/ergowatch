import os
from pathlib import Path
import subprocess


def run_watcher(
    cfg_path: Path, target="release", sync_only=True, backtrace=False
) -> subprocess.CompletedProcess:
    exe = str(
        Path(__file__).parent.parent.absolute() / Path(f"target/{target}/watcher")
    )
    args = [exe, "-c", cfg_path]
    if sync_only:
        args.append("--sync-only")

    env = dict(
        os.environ,
        EW_LOG="DEBUG",
    )
    if backtrace:
        env["RUST_BACKTRACE"] = "full"

    cp = subprocess.run(
        [exe, "-c", cfg_path, "--sync-only"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        timeout=10,
    )
    print(cp.stdout.decode())
    return cp

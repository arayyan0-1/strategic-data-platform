# src/sdp/transform.py
"""Run dbt with the paths that config.Settings holds.

dbt starts from its own directory and it reads its inputs from environment
variables. A relative path in profiles.yml therefore breaks as soon as the
command starts from another working directory. That error happened twice
already with the Python paths.

This module passes settings.data_root and settings.warehouse_path to the dbt
process, so that the lake has one definition and dbt has no second copy of it.
config.py stays the only module that reads the environment.

    python -m sdp.transform build
    python -m sdp.transform build --vars '{ca_pull_policy: latest}'
    python -m sdp.transform test --select stg_prices_adjusted
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from sdp.config import settings

PROJECT_DIR = Path(__file__).resolve().parents[2] / "transform"


def env() -> dict[str, str]:
    out = os.environ.copy()
    out["SDP_DATA_ROOT"] = str(settings.data_root)
    out["SDP_WAREHOUSE"] = str(settings.warehouse_path)
    return out


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        argv = ["build"]
    if not PROJECT_DIR.exists():
        print(f"The dbt project is absent: {PROJECT_DIR}", file=sys.stderr)
        return 1

    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = ["dbt", *argv,
           "--project-dir", str(PROJECT_DIR),
           "--profiles-dir", str(PROJECT_DIR)]
    try:
        return subprocess.call(cmd, env=env())
    except FileNotFoundError:
        print("dbt is not installed. Run: uv sync --group transform",
              file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Run dbt with the paths from config.Settings, so it works from any working directory.

    python -m sdp.transform build
    python -m sdp.transform test --select int_prices_adjusted

dbt never writes the live warehouse. It writes a private file beside it, and a `build` or
`run` that exits 0 replaces the live file and records the publish in `last_build.json`.
Any other result deletes the private file, so readers of the live file never block dbt
and a failed build changes nothing. One dbt run holds the lock at a time.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb

from sdp.config import settings

PROJECT_DIR = Path(__file__).resolve().parents[2] / "transform"

PUBLISH_COMMANDS = {"build", "run"}
"""The dbt commands whose result replaces the live warehouse."""

FULL_BUILD_OPTIONS = {"--full-refresh", "--fail-fast", "-x"}
"""Options that keep a build or run over the whole project. A build with any
other option starts from a copy of the live warehouse, so no model is lost."""

NO_DATABASE_COMMANDS = {"parse", "ls", "list", "deps", "clean", "debug"}
"""dbt commands that do not read the warehouse. They start from an empty file."""

HELP_FLAGS = {"-h", "--help", "--version"}


def env(warehouse: Path | None = None) -> dict[str, str]:
    """Return the environment for dbt; SDP_WAREHOUSE defaults to the live warehouse."""
    out = os.environ.copy()
    out["SDP_DATA_ROOT"] = str(settings.data_root)
    out["SDP_WAREHOUSE"] = str(warehouse or settings.warehouse_path)
    out["SDP_DBT_MEMORY_LIMIT"] = settings.dbt_memory_limit
    out["SDP_DBT_THREADS"] = str(settings.dbt_threads)
    return out


def stamp_path() -> Path:
    """Return the stamp file each published build writes beside the warehouse.

    Its mtime is the start of the last full build.
    """
    return settings.warehouse_path.parent / "last_build.json"


def last_build() -> dict | None:
    """Return the stamp of the last published build, or None if absent or invalid."""
    try:
        stamp = json.loads(stamp_path().read_text(encoding="utf-8"))
        dt.datetime.fromisoformat(stamp["published_utc"])
    except (OSError, ValueError, TypeError, KeyError):
        return None
    return stamp


def _dbt_executable() -> str:
    """Return the dbt next to the running interpreter, so a launchd agent finds it without
    PATH. Fall back to PATH."""
    exe = Path(sys.executable).parent / "dbt"
    return str(exe) if exe.exists() else "dbt"


def _is_full(argv: list[str]) -> bool:
    """Return True when argv builds the whole project: the command, --vars and
    FULL_BUILD_OPTIONS only."""
    rest = iter(argv[1:])
    for a in rest:
        if a == "--vars":
            next(rest, None)
        elif not (a.startswith("--vars=") or a in FULL_BUILD_OPTIONS):
            return False
    return True


# ---------- the lock and the build file ----------

def _lock_dir() -> Path:
    return settings.data_root / "_logs" / ".dbt.lock"


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # The process is alive and owned by another user.
    return True


def _take_lock() -> bool:
    """Claim the one-run lock; return False when a live process holds it.

    The lock directory appears with its pid file in one rename.
    """
    d = _lock_dir()
    d.parent.mkdir(parents=True, exist_ok=True)
    tmp = d.with_name(f"{d.name}.{os.getpid()}")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    (tmp / "pid").write_text(str(os.getpid()))
    for _ in range(2):
        try:
            os.rename(tmp, d)
            return True
        except OSError:
            try:
                holder = int((d / "pid").read_text())
            except (OSError, ValueError):
                holder = None
            if holder is not None and _pid_alive(holder):
                break
            shutil.rmtree(d, ignore_errors=True)  # A dead process left it.
    shutil.rmtree(tmp, ignore_errors=True)
    return False


def _wal(path: Path) -> Path:
    return path.with_name(path.name + ".wal")


def _build_file() -> Path:
    """Return the private build file of this process.

    It sits in a directory beside the live file so os.replace is atomic, and keeps the
    live file name because dbt-duckdb names the database after the file.
    """
    live = settings.warehouse_path
    return live.parent / f"{live.stem}.build-{os.getpid()}" / live.name


def _discard(build: Path) -> None:
    """Delete the build directory: the build file, its WAL and its spill directory."""
    shutil.rmtree(build.parent, ignore_errors=True)


def _remove_orphans() -> None:
    """Delete what killed runs left. The caller holds the lock."""
    live = settings.warehouse_path
    for p in live.parent.glob(f"{live.stem}.build-*"):
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink(missing_ok=True)


def _clone(src: Path, dst: Path) -> None:
    """Copy a file. On macOS, `cp -c` makes an instant APFS clone."""
    if sys.platform == "darwin":
        r = subprocess.run(["cp", "-c", str(src), str(dst)], capture_output=True)
        if r.returncode == 0:
            return
    shutil.copy2(src, dst)


def _start(build: Path, *, empty: bool) -> None:
    """Prepare the build file: empty, or a copy of the live warehouse and its WAL."""
    build.parent.mkdir()
    live = settings.warehouse_path
    if empty or not live.exists():
        return
    _clone(live, build)
    if _wal(live).exists():
        _clone(_wal(live), _wal(build))


# ---------- the publish ----------

def _checkpoint(path: Path) -> None:
    """Write the WAL into the database file so the file is complete alone."""
    with duckdb.connect(str(path)) as con:
        con.execute("checkpoint")


def _publish(build: Path, argv: list[str], full_start: float | None) -> None:
    """Replace the live warehouse with the build file and write the stamp.

    full_start is the start time of a full build, or None for a partial build.
    """
    if _wal(build).exists():
        _checkpoint(build)
    stamp = stamp_path()
    if full_start is not None:
        fresh = full_start
    else:
        # A partial build refreshes some models only: keep the freshness of the last
        # full build, or none.
        fresh = stamp.stat().st_mtime if stamp.exists() else 0.0

    live = settings.warehouse_path
    # A WAL beside the live file belongs to the old file. DuckDB replays a WAL
    # onto the file beside it, so remove the WAL first.
    _wal(live).unlink(missing_ok=True)
    os.replace(build, live)

    tmp = stamp.with_suffix(".json.tmp")
    tmp.write_text(json.dumps({
        "published_utc": dt.datetime.now(dt.UTC).isoformat(),
        "argv": argv,
    }, indent=2), encoding="utf-8")
    # A raw file written after the build starts is newer than the stamp, so the
    # next freshness check reads stale.
    os.utime(tmp, (fresh, fresh))
    os.replace(tmp, stamp)


def _run(argv: list[str]) -> int:
    """Run dbt on a private file and publish it on success. The caller holds the lock."""
    command = argv[0]
    asks_help = bool(HELP_FLAGS & set(argv))
    publish = command in PUBLISH_COMMANDS and not asks_help
    full = publish and _is_full(argv)
    build = _build_file()
    cmd = [_dbt_executable(), *argv,
           "--project-dir", str(PROJECT_DIR),
           "--profiles-dir", str(PROJECT_DIR)]

    _remove_orphans()
    started = time.time()
    try:
        try:
            _start(build, empty=full or asks_help or command in NO_DATABASE_COMMANDS)
        except OSError as exc:
            print(f"The copy of the live warehouse failed: {exc}", file=sys.stderr)
            return 1
        try:
            code = subprocess.call(cmd, env=env(build))
        except FileNotFoundError:
            print("dbt is not installed. Run: uv sync --group transform",
                  file=sys.stderr)
            return 1
        if not publish:
            return code
        if code != 0:
            print(f"dbt stopped with exit code {code}. The live warehouse did "
                  f"not change.", file=sys.stderr)
            return code
        if not build.exists():
            print("dbt wrote no warehouse. The live warehouse did not change.",
                  file=sys.stderr)
            return code
        try:
            _publish(build, argv, started if full else None)
        except (OSError, duckdb.Error) as exc:
            print(f"The publish failed: {exc}", file=sys.stderr)
            return 1
        return code
    finally:
        _discard(build)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv:
        argv = ["build"]
    if not PROJECT_DIR.exists():
        print(f"The dbt project is absent: {PROJECT_DIR}", file=sys.stderr)
        return 1
    if argv[0].startswith("-") and argv[0] not in HELP_FLAGS:
        print("Put the dbt command first. Example: python -m sdp.transform "
              "build --debug", file=sys.stderr)
        return 2
    if argv[0] == "retry":
        print("dbt retry cannot work here. A failed build keeps no result. Run "
              "the build again.", file=sys.stderr)
        return 2

    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    if not _take_lock():
        print(f"Another dbt run holds the lock: {_lock_dir()}", file=sys.stderr)
        return 1
    try:
        return _run(argv)
    finally:
        shutil.rmtree(_lock_dir(), ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())

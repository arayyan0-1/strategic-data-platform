# src/sdp/daily.py
"""The daily driver, one command for a scheduler.

    python -m sdp.daily

Three steps. (1) Pull the current-state datasets (splits, dividends); each pull
replaces the whole table, so a missed day costs nothing. (2) Fill the event
streams (day_aggs, tickers) from the day after the last partition, so a machine
that slept self-heals. Step 2 is capped so a long gap does not become a
backfill. (3) Build the dbt models, but only when a new session landed, so raw
data becomes usable without a second command. `update()` runs all three under a
lock. `run()` is steps 1 and 2 alone. Exits 1 on any failure.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import shutil
import sys
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path

from sdp import backfill, dal
from sdp.config import settings
from sdp.ingest import massive_corporate_actions as ca

log = logging.getLogger("sdp.daily")

CURRENT_STATE = ["massive_splits", "massive_dividends"]

EVENT_STREAMS = {
    "day_aggs": dal.DAY_AGGS,
    "tickers": dal.TICKERS,
    # Short interest reports on a two-week cadence, so most sessions publish
    # nothing and the fill retries them cheaply on each run.
    "short_volume": dal.SHORT_VOLUME,
    "short_interest": dal.SHORT_INTEREST,
}

DEFAULT_MAX_SESSIONS = 30
"""The most sessions that one daily run fills for each event stream."""

COLD_START_DAYS = 7
"""How far back to start when an event stream has no partition at all.

A cold start must not begin a five-year backfill by accident. Run
`python -m sdp.backfill` for that.
"""


def _fill_start(ds: dal.Dataset, today: dt.date) -> dt.date:
    """The date the fill of an event stream starts from. A cold stream starts
    COLD_START_DAYS back, not at the window floor."""
    parts = dal.partitions(ds)
    if parts:
        return parts[-1] + dt.timedelta(days=1)
    return today - dt.timedelta(days=COLD_START_DAYS)


def _pending_count(ds: dal.Dataset, today: dt.date, max_sessions: int) -> int:
    """How many sessions the next fill of this stream will process."""
    start = _fill_start(ds, today)
    if start > today:
        return 0
    return min(len(backfill.sessions(start, today)), max_sessions)


def _pull_current_state(today: dt.date, *, force: bool = False,
                        on_event: Callable[[dict], None] | None = None) -> list[str]:
    """Pull each current-state dataset for today. Return the names that failed."""
    failed = []
    for dataset in CURRENT_STATE:
        if on_event:
            on_event({"kind": "pull_start", "dataset": dataset})
        try:
            path = ca.ingest(dataset, today, force=force)
            log.info("%s: %s", dataset, path)
            ok = True
        except Exception as exc:  # noqa: BLE001 -- one dataset must not stop the other
            failed.append(dataset)
            log.error("%s FAILED %s: %s", dataset, type(exc).__name__, exc)
            ok = False
        if on_event:
            on_event({"kind": "pull_done", "dataset": dataset, "ok": ok})
    return failed


def _fill_event_stream(
    name: str,
    ds: dal.Dataset,
    today: dt.date,
    max_sessions: int,
    on_event: Callable[[dict], None] | None = None,
) -> list[tuple[dt.date, str]]:
    """Publish every missing session up to today. Return the failures."""
    parts = dal.partitions(ds)
    start = _fill_start(ds, today)
    if not parts:
        log.warning("%s has no partition. The fill starts at %s. Run "
                    "python -m sdp.backfill for a full backfill.", name, start)

    if start > today:
        log.info("%s is up to date at %s.", name, parts[-1])
        if on_event:
            on_event({"kind": "stream_start", "dataset": name, "pending": 0})
        return []

    pending = backfill.sessions(start, today)
    if not pending:
        log.info("%s: no XNYS session between %s and %s.", name, start, today)
        if on_event:
            on_event({"kind": "stream_start", "dataset": name, "pending": 0})
        return []
    if len(pending) > max_sessions:
        log.warning("%s has %s sessions to fill and the limit is %s. The rest "
                    "closes over the runs of the next days.",
                    name, len(pending), max_sessions)

    if on_event:
        on_event({"kind": "stream_start", "dataset": name,
                  "pending": min(len(pending), max_sessions)})

    def _sess(d: dt.date, status: str) -> None:
        if on_event:
            on_event({"kind": "session", "dataset": name, "date": d, "status": status})

    return backfill.backfill(name, start, today, limit=max_sessions, on_session=_sess)


def run(
    today: dt.date | None = None,
    *,
    max_sessions: int = DEFAULT_MAX_SESSIONS,
    skip_current_state: bool = False,
    skip_event_streams: bool = False,
    force: bool = False,
    on_event: Callable[[dict], None] | None = None,
) -> int:
    """Run the daily job. Return the exit code. on_event reports each step for a
    progress meter."""
    today = today or dt.datetime.now(dt.UTC).date()
    log.info("Daily run for %s.", today)

    problems: list[str] = []

    if not skip_current_state:
        failed = _pull_current_state(today, force=force, on_event=on_event)
        problems += [f"{name} pull failed" for name in failed]

    if not skip_event_streams:
        for name, ds in EVENT_STREAMS.items():
            failures = _fill_event_stream(name, ds, today, max_sessions, on_event)
            problems += [f"{name} {d}: {msg}" for d, msg in failures]

    print("\n" + dal.status())
    if problems:
        print(f"\n{len(problems)} problems:")
        for p in problems[:20]:
            print(f"  {p}")
        return 1
    print("\nThe daily run finished with no problem.")
    return 0


# ---------- the lock ----------

class UpdateInProgress(RuntimeError):
    """Another update holds the lock."""


def _logs_dir() -> Path:
    return settings.data_root / "_logs"


def _lock_dir() -> Path:
    return _logs_dir() / ".update.lock"


def _backfill_lock_dir() -> Path:
    """The lock that run_backfill.sh holds. An update must not write while a
    backfill runs, because both write vendor/ and _staging/."""
    return _logs_dir() / ".backfill.lock"


def _lock_is_stale(d: Path) -> bool:
    """True when the lock names a process that is gone."""
    try:
        pid = int((d / "pid").read_text())
    except (OSError, ValueError):
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False  # The process is alive and owned by another user.
    return False


@contextmanager
def _update_lock():
    """A one-writer lock. mkdir is the test and the claim in one step, the same
    pattern as run_backfill.sh. A dead process leaves a stale lock, which the
    next update reclaims."""
    backfill_lock = _backfill_lock_dir()
    if backfill_lock.exists() and not _lock_is_stale(backfill_lock):
        raise UpdateInProgress(f"A backfill is in progress. Lock: {backfill_lock}")
    d = _lock_dir()
    d.parent.mkdir(parents=True, exist_ok=True)
    try:
        d.mkdir()
    except FileExistsError:
        if not _lock_is_stale(d):
            raise UpdateInProgress(f"An update is in progress. Lock: {d}") from None
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir()
    (d / "pid").write_text(str(os.getpid()))
    try:
        yield
    finally:
        shutil.rmtree(d, ignore_errors=True)


# ---------- freshness of the dbt build ----------

def _newest_raw_mtime() -> float | None:
    """The mtime of the newest file in raw/, or None when raw/ is empty."""
    root = settings.raw_dir
    if not root.exists():
        return None
    newest = None
    for p in root.rglob("*.parquet"):
        m = p.stat().st_mtime
        if newest is None or m > newest:
            newest = m
    return newest


def dbt_stale() -> bool:
    """True when the dbt build is older than the newest raw file, or absent. A
    fresh raw partition with an older warehouse means the models must run."""
    wh = settings.warehouse_path
    if not wh.exists():
        return True
    raw = _newest_raw_mtime()
    if raw is None:
        return False
    return wh.stat().st_mtime < raw


def _build_dbt() -> int:
    """Run the dbt build. A thin wrapper, so a test can replace it."""
    from sdp import transform
    return transform.main(["build"])


# ---------- the status snapshot ----------

def _iso(d: dt.date | None) -> str | None:
    return d.isoformat() if d else None


def _expected_last_session(now_utc: dt.datetime) -> dt.date | None:
    """The newest XNYS session whose S3 flat file should exist now. Session D's
    file lands about 06:00 UTC on D+1, so before 06:30 UTC the day before
    yesterday is the safe answer."""
    through = now_utc.date() - dt.timedelta(days=1)
    if now_utc.time() < dt.time(6, 30):
        through -= dt.timedelta(days=1)
    days = backfill.sessions(through - dt.timedelta(days=12), through)
    return days[-1] if days else None


def _sessions_behind(last: dt.date | None, expected: dt.date | None) -> int | None:
    """XNYS sessions between the last partition and the expected last session."""
    if last is None or expected is None:
        return None
    if last >= expected:
        return 0
    return len(backfill.sessions(last + dt.timedelta(days=1), expected))


def _read_last_pull() -> dict | None:
    """The last_pull block of the previous status file, or None."""
    p = _logs_dir() / "status.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8")).get("last_pull")
    except (OSError, ValueError):
        return None


def _mtime_iso(path: Path) -> str | None:
    """When a file last changed, as an ISO timestamp. This is the last time a
    dataset had a successful write."""
    try:
        return dt.datetime.fromtimestamp(path.stat().st_mtime, dt.UTC).isoformat()
    except OSError:
        return None


def status_snapshot(now_utc: dt.datetime | None = None) -> dict:
    """Describe what each dataset holds and what is missing. The dashboard reads
    this. Each dataset kind has its own idea of "behind", so a sparse dataset
    does not raise a false alarm."""
    now_utc = now_utc or dt.datetime.now(dt.UTC)
    expected = _expected_last_session(now_utc)
    last_pull = _read_last_pull()
    datasets = []

    # Daily event streams. "Behind" is the count of sessions not yet published.
    for name, ds in (("day_aggs", dal.DAY_AGGS),
                     ("tickers", dal.TICKERS),
                     ("short_volume", dal.SHORT_VOLUME)):
        cov = dal.coverage(ds)
        last = cov[1] if cov else None
        behind = _sessions_behind(last, expected)
        level = "bad" if behind is None else ("ok" if behind == 0 else "warn")
        updated = _mtime_iso(ds.partition_file(last)) if last else None
        datasets.append({"name": name, "kind": "event", "last": _iso(last),
                         "behind": behind, "level": level, "updated": updated,
                         "detail": f"through {last}" if last else "no partitions"})

    # Short interest is sparse and lagged, so it never reports "behind". It is
    # healthy when a pull checked it today.
    cov = dal.coverage(dal.SHORT_INTEREST)
    last = cov[1] if cov else None
    checked_today = bool(
        last_pull and last_pull.get("time", "")[:10] == now_utc.date().isoformat()
    )
    updated = _mtime_iso(dal.SHORT_INTEREST.partition_file(last)) if last else None
    datasets.append({"name": "short_interest", "kind": "sparse", "last": _iso(last),
                     "behind": None, "level": "ok" if checked_today else "warn",
                     "updated": updated,
                     "detail": f"last settlement {last}" if last else "no settlements"})

    # Current state. A missed day costs nothing, so it is never "bad".
    for name, ds in (("splits", dal.SPLITS), ("dividends", dal.DIVIDENDS)):
        f = ds.table_file
        if f.exists():
            when = dt.datetime.fromtimestamp(f.stat().st_mtime, dt.UTC).date()
            age = (now_utc.date() - when).days
            datasets.append({"name": name, "kind": "current", "behind": None,
                             "level": "ok" if age == 0 else "warn",
                             "updated": _mtime_iso(f),
                             "detail": f"refreshed {when}"})
        else:
            datasets.append({"name": name, "kind": "current", "behind": None,
                             "level": "warn", "updated": None,
                             "detail": "not pulled"})

    # The dbt build.
    wh = settings.warehouse_path
    if wh.exists():
        built = dt.datetime.fromtimestamp(wh.stat().st_mtime, dt.UTC).date()
        stale = dbt_stale()
        dbt = {"built": _iso(built), "stale": stale,
               "level": "bad" if stale else "ok",
               "detail": f"built {built}"}
    else:
        dbt = {"built": None, "stale": True, "level": "bad", "detail": "never built"}

    return {"generated": now_utc.isoformat(),
            "expected_last_session": _iso(expected),
            "datasets": datasets, "dbt": dbt, "last_pull": last_pull}


def _write_status(snap: dict) -> None:
    p = _logs_dir() / "status.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(snap, indent=2), encoding="utf-8")
    os.replace(tmp, p)


# ---------- the shared update job ----------

def _plan_total(today: dt.date, max_sessions: int, *,
                skip_current_state: bool, skip_event_streams: bool) -> int:
    """Count the units of work, so the dashboard can draw a real progress bar.
    One unit is a current-state pull or one session, plus one for the dbt step."""
    total = 0 if skip_current_state else len(CURRENT_STATE)
    if not skip_event_streams:
        for ds in EVENT_STREAMS.values():
            total += _pending_count(ds, today, max_sessions)
    return total + 1  # The dbt step is always one unit, built or skipped.


def update(
    today: dt.date | None = None,
    *,
    max_sessions: int = DEFAULT_MAX_SESSIONS,
    force: bool = False,
    force_dbt: bool = False,
    skip_current_state: bool = False,
    skip_event_streams: bool = False,
    progress: Callable[[dict], None] | None = None,
) -> dict:
    """Pull the datasets, build the dbt models when a new session landed, and
    write the status file. One writer at a time. The scheduler and the dashboard
    button both call this. progress reports each step as a dict with a message
    and a done/total count. Raises UpdateInProgress when the lock is held."""
    today = today or dt.datetime.now(dt.UTC).date()
    total = _plan_total(today, max_sessions, skip_current_state=skip_current_state,
                        skip_event_streams=skip_event_streams)
    done = 0

    def emit(message: str, *, dataset: str | None = None) -> None:
        log.info(message)
        if progress:
            progress({"message": message, "dataset": dataset,
                      "done": done, "total": total})

    def on_event(ev: dict) -> None:
        nonlocal done
        kind = ev["kind"]
        if kind == "pull_start":
            emit(f"Pulling {ev['dataset']}", dataset=ev["dataset"])
        elif kind == "pull_done":
            done += 1
            emit(f"Pulled {ev['dataset']}", dataset=ev["dataset"])
        elif kind == "stream_start":
            n = ev["pending"]
            emit(f"Filling {ev['dataset']}, {n} session{'' if n == 1 else 's'}"
                 if n else f"{ev['dataset']} is up to date", dataset=ev["dataset"])
        elif kind == "session":
            done += 1
            emit(f"{ev['dataset']} {ev['date']} {ev['status']}", dataset=ev["dataset"])

    with _update_lock():
        code = run(today, max_sessions=max_sessions,
                   skip_current_state=skip_current_state,
                   skip_event_streams=skip_event_streams, force=force,
                   on_event=on_event)

        if force_dbt or dbt_stale():
            emit("Building the dbt models", dataset="dbt")
            dbt = "built" if _build_dbt() == 0 else "failed"
        else:
            dbt = "skipped"
            emit("The dbt models are current", dataset="dbt")
        done += 1

        snap = status_snapshot()
        snap["last_pull"] = {"time": dt.datetime.now(dt.UTC).isoformat(),
                             "exit_code": code, "dbt": dbt}
        _write_status(snap)
        emit("The dbt build failed. Run: uv sync --group transform"
             if dbt == "failed" else "Done")
        return snap


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m sdp.daily",
        description="Pull the current-state datasets and fill the event streams.",
    )
    p.add_argument("--date", type=dt.date.fromisoformat,
                   help="Run as if today were this date, as YYYY-MM-DD.")
    p.add_argument("--max-sessions", type=int, default=DEFAULT_MAX_SESSIONS,
                   help=f"Sessions to fill for each event stream. "
                        f"Default {DEFAULT_MAX_SESSIONS}.")
    p.add_argument("--skip-current-state", action="store_true",
                   help="Do not pull the splits and the dividends.")
    p.add_argument("--skip-event-streams", action="store_true",
                   help="Do not fill the day aggregates and the tickers.")
    p.add_argument("--force", action="store_true",
                   help="Pull the current-state datasets again for today.")
    p.add_argument("--force-dbt", action="store_true",
                   help="Build the dbt models even when no new session landed.")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Quiet httpx's one-INFO-line-per-request. Retries and failures still log
    # from the sdp loggers.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        snap = update(
            args.date,
            max_sessions=args.max_sessions,
            force=args.force,
            force_dbt=args.force_dbt,
            skip_current_state=args.skip_current_state,
            skip_event_streams=args.skip_event_streams,
        )
    except UpdateInProgress as exc:
        log.warning("%s", exc)
        return 0  # Another writer has it. This is not a failure.
    return snap["last_pull"]["exit_code"]


if __name__ == "__main__":
    sys.exit(main())

# src/sdp/daily.py
"""The daily driver, one command for a scheduler.

    python -m sdp.daily

Three steps. (1) Pull the current-state datasets (splits, dividends). Each pull
replaces the whole table, so a missed day costs nothing. (2) Fill the four event
streams (day_aggs, tickers, short_volume, short_interest) from the day after the
last partition, so a machine that slept self-heals. Step 2 is capped so a long
gap does not become a backfill. (3) Build the dbt models, but only when a new
session landed, so raw data becomes usable without a second command. `update()`
runs all three under a lock. `run()` is steps 1 and 2 alone. When the vendor
hosts do not resolve, `update()` skips steps 1 and 2 and exits 0. Otherwise it
exits 1 when a pull fails.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import shutil
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

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

ALERT_AFTER = 2
"""The count of failed runs in a row that sends a desktop notification."""

OFFLINE_ALERT_HOURS = 24
"""The hours of offline runs in a row that send a desktop notification."""

LOG_KEEP_DAYS = 30
"""Backfill run logs older than this many days are deleted."""

LOG_ROTATE_BYTES = 5 * 1024 * 1024
"""A launchd log larger than this is renamed to *.log.1."""

ROTATED_LOGS = ("daily.out.log", "daily.err.log")
"""The launchd logs of this job, which the pruning rotates."""

COPIED_LOGS = ("dashboard.out.log", "dashboard.err.log")
"""The logs of the dashboard. Its process keeps them open, so the pruning copies
and truncates them in place instead of a rename."""


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


def _fill_end(name: str, today: dt.date, now_utc: dt.datetime | None = None) -> dt.date:
    """The last date that the fill of an event stream asks for. The S3 flat file
    of a session lands on the next day, so day_aggs stops at the newest session
    whose file must exist."""
    if name != "day_aggs":
        return today
    expected = _expected_last_session(now_utc or dt.datetime.now(dt.UTC))
    return min(today, expected) if expected else today


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
    problems: list[str] | None = None,
) -> int:
    """Run the daily job. Return the exit code. on_event reports each step for a
    progress meter. When problems is a list, the run adds each problem to it."""
    today = today or dt.datetime.now(dt.UTC).date()
    log.info("Daily run for %s.", today)

    found: list[str] = []

    if not skip_current_state:
        failed = _pull_current_state(today, force=force, on_event=on_event)
        found += [f"{name} pull failed" for name in failed]

    if not skip_event_streams:
        for name, ds in EVENT_STREAMS.items():
            failures = _fill_event_stream(name, ds, _fill_end(name, today),
                                          max_sessions, on_event)
            found += [f"{name} {d}: {msg}" for d, msg in failures]

    if problems is not None:
        problems.extend(found)

    print("\n" + dal.status())
    if found:
        print(f"\n{len(found)} problems:")
        for p in found[:20]:
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
    """True when no build is published, or when the last full build started
    before the newest raw file. A failed build does not write the stamp, so the
    warehouse stays stale."""
    from sdp import transform
    stamp = transform.stamp_path()
    if not settings.warehouse_path.exists() or not stamp.exists():
        return True
    raw = _newest_raw_mtime()
    if raw is None:
        return False
    return stamp.stat().st_mtime < raw


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


def status_snapshot(now_utc: dt.datetime | None = None,
                    last_pull: dict | None = None) -> dict:
    """Describe what each dataset holds and what is missing. The dashboard reads
    this. Each dataset kind has its own idea of "behind", so a sparse dataset
    does not raise a false alarm. last_pull defaults to the one in status.json."""
    now_utc = now_utc or dt.datetime.now(dt.UTC)
    expected = _expected_last_session(now_utc)
    last_pull = _read_last_pull() if last_pull is None else last_pull
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
    # healthy when a clean pull that was not offline checked it today.
    cov = dal.coverage(dal.SHORT_INTEREST)
    last = cov[1] if cov else None
    checked_today = bool(
        last_pull
        and str(last_pull.get("time") or "")[:10] == now_utc.date().isoformat()
        and last_pull.get("exit_code") == 0
        and not last_pull.get("offline")
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

    # The dbt build. The stamp of the last publish dates it.
    from sdp import transform
    stamp = transform.last_build()
    if settings.warehouse_path.exists() and stamp:
        built = dt.datetime.fromisoformat(stamp["published_utc"]).date()
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


# ---------- the offline probe ----------

def _online() -> bool:
    """True when one or more vendor hosts resolve. launchd runs a missed tick on
    wake, often before the network is up. When only one host fails, its pulls
    run and fail, so the failure count shows the problem."""
    for url in (settings.massive_api_base, settings.massive_s3_endpoint):
        host = urlsplit(url).hostname or url
        try:
            socket.getaddrinfo(host, 443)
            return True
        except (OSError, UnicodeError):
            continue
    return False


# ---------- the failure signal ----------

def _failures_in_a_row(prev: dict, *, failed: bool, pulled: bool) -> int:
    """Count the failed runs in a row. A clean run that pulled sets the count to
    0. A run that did not pull, for example an offline run, keeps it."""
    n = prev.get("failures_in_a_row")
    n = n if isinstance(n, int) else 0
    if failed:
        return n + 1
    return 0 if pulled else n


def _offline_since(prev: dict, offline: bool, now: dt.datetime) -> str | None:
    """The time of the first offline run in the current series, or None."""
    if not offline:
        return None
    since = prev.get("offline_since")
    return since if isinstance(since, str) else now.isoformat()


def _alert_message(*, failures: int, failed: bool, offline_since: str | None,
                   now: dt.datetime) -> str | None:
    """The text of the alert that this run must send, or None."""
    if failed and failures >= ALERT_AFTER:
        return (f"The daily update failed {failures} times in a row. "
                f"See data/_logs/daily.err.log.")
    if offline_since:
        try:
            hours = (now - dt.datetime.fromisoformat(offline_since)).total_seconds() / 3600
        except (TypeError, ValueError):
            return None
        if hours >= OFFLINE_ALERT_HOURS:
            return (f"The vendor hosts did not resolve for {int(hours)} hours. "
                    f"The daily update did not pull.")
    return None


def _notify(message: str) -> None:
    """Show a macOS notification. An error only logs, because the alert must
    not stop the update."""
    text = message.replace("\\", "\\\\").replace('"', '\\"')
    try:
        subprocess.run(
            ["osascript", "-e", f'display notification "{text}" with title "sdp"'],
            check=False, capture_output=True, timeout=10,
        )
    except Exception as exc:  # noqa: BLE001 -- a failed alert must not fail the run
        log.warning("The notification failed. %s: %s", type(exc).__name__, exc)


def _alert(message: str | None, prev: dict, today: str) -> str | None:
    """Send the message as a notification, one each day at most. Return the ISO
    date of the last alert."""
    last = prev.get("last_alert")
    if message is None or last == today:
        return last
    _notify(message)
    return today


# ---------- log pruning ----------

def _prune_one(p: Path, cutoff: float) -> None:
    """Delete an old backfill run log, or move a large launchd log to *.log.1."""
    st = p.stat()
    if p.suffix == ".jsonl":
        if st.st_mtime < cutoff:
            p.unlink(missing_ok=True)
    elif st.st_size > LOG_ROTATE_BYTES:
        if p.name in COPIED_LOGS:
            shutil.copyfile(p, p.with_name(f"{p.name}.1"))
            os.truncate(p, 0)
        else:
            os.replace(p, p.with_name(f"{p.name}.1"))


def _prune_logs() -> None:
    """Delete the backfill run logs older than LOG_KEEP_DAYS and rotate a large
    launchd log. An error only logs, because pruning must not fail the run."""
    d = _logs_dir()
    cutoff = time.time() - LOG_KEEP_DAYS * 86400
    try:
        paths = [*d.glob("backfill_*.jsonl"),
                 *(d / n for n in (*ROTATED_LOGS, *COPIED_LOGS) if (d / n).exists())]
    except Exception as exc:  # noqa: BLE001 -- pruning must not fail the run
        log.warning("The log pruning failed. %s: %s", type(exc).__name__, exc)
        return
    for p in paths:
        try:
            _prune_one(p, cutoff)
        except FileNotFoundError:
            pass  # Another process removed it.
        except Exception as exc:  # noqa: BLE001 -- pruning must not fail the run
            log.warning("The log pruning failed for %s. %s: %s",
                        p.name, type(exc).__name__, exc)


# ---------- the shared update job ----------

def _plan_total(today: dt.date, max_sessions: int, *,
                skip_current_state: bool, skip_event_streams: bool) -> int:
    """Count the units of work, so the dashboard can draw a real progress bar.
    One unit is a current-state pull or one session, plus one for the dbt step."""
    total = 0 if skip_current_state else len(CURRENT_STATE)
    if not skip_event_streams:
        for name, ds in EVENT_STREAMS.items():
            total += _pending_count(ds, _fill_end(name, today), max_sessions)
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
    and a done/total count. When the vendor hosts do not resolve, the pulls are
    skipped and the exit code is 0. Raises UpdateInProgress when the lock is
    held."""
    today = today or dt.datetime.now(dt.UTC).date()
    done = 0
    total = 1  # The dbt step. The plan adds the pulls when the machine is online.

    def emit(message: str, *, dataset: str | None = None,
             level: int = logging.INFO) -> None:
        log.log(level, message)
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
        wants_pull = not (skip_current_state and skip_event_streams)
        offline = wants_pull and not _online()
        problems: list[str] = []
        if offline:
            code = 0
            emit("The vendor hosts do not resolve. The machine is offline, so "
                 "this run does not pull.", level=logging.WARNING)
        else:
            try:
                total = _plan_total(today, max_sessions,
                                    skip_current_state=skip_current_state,
                                    skip_event_streams=skip_event_streams)
                code = run(today, max_sessions=max_sessions,
                           skip_current_state=skip_current_state,
                           skip_event_streams=skip_event_streams, force=force,
                           on_event=on_event, problems=problems)
            except Exception as exc:  # noqa: BLE001 -- a crash must count as a failure
                log.exception("The daily run stopped with an error.")
                code = 1
                problems.append(f"run stopped: {type(exc).__name__}: {exc}")

        if force_dbt or dbt_stale():
            emit("Building the dbt models", dataset="dbt")
            try:
                dbt = "built" if _build_dbt() == 0 else "failed"
            except Exception:  # noqa: BLE001 -- a crash must count as a failure
                log.exception("The dbt build stopped with an error.")
                dbt = "failed"
        else:
            dbt = "skipped"
            emit("The dbt models are current", dataset="dbt")
        done += 1
        if dbt == "failed":
            problems.insert(0, "dbt build failed")

        now = dt.datetime.now(dt.UTC)
        prev = _read_last_pull()
        prev = prev if isinstance(prev, dict) else {}
        pulled = wants_pull and not offline
        failed = (pulled and code != 0) or dbt == "failed"
        failures = _failures_in_a_row(prev, failed=failed, pulled=pulled)
        offline_since = _offline_since(prev, offline, now)
        message = _alert_message(failures=failures, failed=failed,
                                 offline_since=offline_since, now=now)
        last_pull = {
            "time": now.isoformat(), "exit_code": code, "dbt": dbt,
            "offline": offline, "offline_since": offline_since,
            "failures_in_a_row": failures, "problems": problems[:20],
            "last_alert": _alert(message, prev, now.date().isoformat()),
        }
        try:
            snap = status_snapshot(now, last_pull)
        except Exception as exc:  # noqa: BLE001 -- the status file must still record the run
            log.exception("The status snapshot stopped with an error.")
            snap = {"generated": now.isoformat(), "datasets": [], "dbt": {},
                    "error": f"{type(exc).__name__}: {exc}"}
        snap["last_pull"] = last_pull
        _write_status(snap)
        _prune_logs()
        emit("The dbt build failed. See data/_logs/daily.out.log or "
             "transform/logs/dbt.log." if dbt == "failed" else "Done")
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
                   help="Do not fill the four event streams: day_aggs, tickers, "
                        "short_volume and short_interest.")
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

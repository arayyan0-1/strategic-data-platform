"""Pull new data, fill gaps and rebuild the warehouse if anything changed.

    python -m sdp.daily

`update()` runs three steps under a one-writer lock. (1) Pull the current-state datasets
(splits and dividends, FRED, the Fama-French factors, the q-factors); each pull replaces
its table, so a missed day costs nothing. (2) Fill the event streams from the day after
the last partition, capped per run so a long gap doesn't become a backfill, then the
monthly ticker details. (3) Build dbt when a new session landed. `run()` is steps 1 and 2.

When the vendor hosts don't resolve, `update()` skips the pulls and exits 0. Otherwise it
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
from sdp.ingest import (
    etf_funds,
    etf_holdings,
    etf_nav_history,
    fred,
    french,
    french_international,
    ibkr_borrow,
    openfigi,
    qfactors,
    sec_nport,
)
from sdp.ingest import massive_corporate_actions as ca
from sdp.ingest import massive_ticker_details as details

log = logging.getLogger("sdp.daily")

# Public current-state sources. Each pulls when its table is older than the MAX_AGE_DAYS
# of its module.
PUBLIC = {m.DATASET.name: m for m in (french, french_international, fred, qfactors)}
CURRENT_STATE = ["massive_splits", "massive_dividends", *PUBLIC]

EVENT_STREAMS = {
    "day_aggs": dal.DAY_AGGS,
    "tickers": dal.TICKERS,
    # Short interest reports every two weeks, so most sessions publish nothing and the
    # fill retries them cheaply on each run.
    "short_volume": dal.SHORT_VOLUME,
    "short_interest": dal.SHORT_INTEREST,
}

# Event streams with one partition per month, filled after the daily streams.
MONTHLY_STREAMS = {"ticker_details": dal.TICKER_DETAILS}

# Funding and ETF data, pulled after the event streams. The borrow file and the screeners
# keep no history, so each run takes today's snapshot when it is due.
FUND_DATA = ("ibkr_borrow", "etf_funds", "etf_nav_history", "etf_holdings", "sec_nport",
             "openfigi")

# N-PORT: a fund's filing list is read again after this many hours, and one run maps at most
# this many new identifiers at OpenFIGI (10 to a request, 24 requests a minute keyless).
NPORT_MIN_AGE_HOURS = 168
OPENFIGI_LIMIT = 500

HOLDINGS_FAILURES_TO_REPORT = 5
"""Fewer failed holdings files than this only log: a closed fund must not fail every run."""

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
    """Return the date the fill of an event stream starts from.

    A cold stream starts COLD_START_DAYS back, not at the window floor.
    """
    parts = dal.partitions(ds)
    if parts:
        return parts[-1] + dt.timedelta(days=1)
    return today - dt.timedelta(days=COLD_START_DAYS)


def _pending_count(ds: dal.Dataset, today: dt.date, max_sessions: int) -> int:
    """Return how many sessions the next fill of this stream will process."""
    start = _fill_start(ds, today)
    if start > today:
        return 0
    return min(len(backfill.sessions(start, today)), max_sessions)


def _fill_end(name: str, today: dt.date, now_utc: dt.datetime | None = None) -> dt.date:
    """Return the last date the fill of an event stream asks for.

    The S3 flat file of a session lands the next day, so day_aggs stops at the newest
    session whose file must exist.
    """
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
            if dataset in PUBLIC:
                path = PUBLIC[dataset].ingest(today, force=force)
            else:
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


def _pending_month_ends(today: dt.date) -> list[dt.date]:
    """Return the month-end sessions after the last ticker-details partition whose
    tickers and day_aggs partitions are published.

    A lake with no partition starts 45 days back. Run python -m sdp.backfill
    ticker_details for a full history.
    """
    parts = dal.partitions(dal.TICKER_DETAILS)
    start = parts[-1] + dt.timedelta(days=1) if parts else today - dt.timedelta(days=45)
    return [d for d in details.month_end_sessions(start, today)
            if dal.TICKERS.partition_file(d).exists() and dal.DAY_AGGS.partition_file(d).exists()]


def _fill_ticker_details(today: dt.date,
                         on_event: Callable[[dict], None] | None = None) -> list[str]:
    """Publish the ticker details of each pending month-end. Return the problems."""
    pending = _pending_month_ends(today)
    if on_event:
        on_event({"kind": "stream_start", "dataset": "ticker_details", "pending": len(pending)})
    found = []
    for d in pending:
        try:
            details.ingest(d)
            status = "ok"
        except Exception as exc:  # noqa: BLE001 -- one month must not stop the run
            found.append(f"ticker_details {d}: {type(exc).__name__}: {exc}")
            log.error("ticker_details %s FAILED %s: %s", d, type(exc).__name__, exc)
            status = "failed"
        if on_event:
            on_event({"kind": "session", "dataset": "ticker_details", "date": d,
                      "status": status})
    return found


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


def _pull_nport(now: dt.datetime) -> list[str]:
    """Pull new N-PORT filings, or log and skip when no SEC contact is set."""
    if not settings.sec_user_agent:
        log.info("sec_nport skipped: SEC_USER_AGENT is not set.")
        return []
    return sec_nport.ingest(min_age_hours=NPORT_MIN_AGE_HOURS, now=now)


def _pull_fund_data(now: dt.datetime,
                    on_event: Callable[[dict], None] | None = None) -> list[str]:
    """Take the borrow, screener, NAV-history and holdings pulls that are due. Return the
    problems."""
    found = []
    steps = {
        "ibkr_borrow": lambda: ibkr_borrow.ingest(now),
        "etf_funds": lambda: etf_funds.ingest(now),
        "etf_nav_history": lambda: etf_nav_history.ingest(now.date()),
        "etf_holdings": lambda: etf_holdings.ingest(now),
        "sec_nport": lambda: _pull_nport(now),
        "openfigi": lambda: openfigi.ingest(limit=OPENFIGI_LIMIT),
    }
    for name in FUND_DATA:
        if on_event:
            on_event({"kind": "pull_start", "dataset": name})
        ok = True
        try:
            result = steps[name]()
            if name in ("etf_holdings", "sec_nport"):
                for p in result:
                    log.warning(p)
                if len(result) >= HOLDINGS_FAILURES_TO_REPORT:
                    found.append(f"{name}: {len(result)} funds failed")
                    ok = False
        except Exception as exc:  # noqa: BLE001 -- one source must not stop the others
            found.append(f"{name} pull failed: {type(exc).__name__}: {exc}")
            log.error("%s FAILED %s: %s", name, type(exc).__name__, exc)
            ok = False
        if on_event:
            on_event({"kind": "pull_done", "dataset": name, "ok": ok})
    return found


def run(
    today: dt.date | None = None,
    *,
    max_sessions: int = DEFAULT_MAX_SESSIONS,
    skip_current_state: bool = False,
    skip_event_streams: bool = False,
    skip_fund_data: bool = False,
    force: bool = False,
    on_event: Callable[[dict], None] | None = None,
    problems: list[str] | None = None,
) -> int:
    """Run the daily job and return the exit code.

    on_event reports each step for a progress meter. When problems is a list, the run
    adds each problem to it.
    """
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
        # Monthly, after the tickers and bars of the month-end are published.
        found += _fill_ticker_details(today, on_event)

    if not skip_fund_data:
        found += _pull_fund_data(dt.datetime.now(dt.UTC), on_event)

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
    """Return the lock that run_backfill.sh holds.

    An update must not write while a backfill runs, because both write vendor/ and
    _staging/.
    """
    return _logs_dir() / ".backfill.lock"


def _lock_is_stale(d: Path) -> bool:
    """Return True when the lock names a process that is gone."""
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


def _lock_holder(d: Path) -> str:
    """Return " since <time> UTC (pid <n>)" for a lock, or "" when the time is unknown.
    The time is the mtime of the pid file, else of the lock directory."""
    pid_file = d / "pid"
    try:
        mtime = pid_file.stat().st_mtime
    except OSError:
        try:
            mtime = d.stat().st_mtime
        except OSError:
            return ""
    since = dt.datetime.fromtimestamp(mtime, dt.UTC).strftime("%Y-%m-%d %H:%M UTC")
    try:
        pid = f" (pid {int(pid_file.read_text())})"
    except (OSError, ValueError):
        pid = ""
    return f" since {since}{pid}"


@contextmanager
def _update_lock():
    """Take the one-writer lock.

    mkdir is the test and the claim in one step, as in run_backfill.sh. A dead process
    leaves a stale lock, which the next update reclaims.
    """
    backfill_lock = _backfill_lock_dir()
    if backfill_lock.exists() and not _lock_is_stale(backfill_lock):
        raise UpdateInProgress(f"A backfill is in progress{_lock_holder(backfill_lock)}. "
                               f"Lock: {backfill_lock}")
    d = _lock_dir()
    d.parent.mkdir(parents=True, exist_ok=True)
    try:
        d.mkdir()
    except FileExistsError:
        if not _lock_is_stale(d):
            raise UpdateInProgress(f"An update is in progress{_lock_holder(d)}. "
                                   f"Lock: {d}") from None
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir()
    (d / "pid").write_text(str(os.getpid()))
    try:
        yield
    finally:
        shutil.rmtree(d, ignore_errors=True)


# ---------- freshness of the dbt build ----------

def _newest_raw_mtime() -> float | None:
    """Return the mtime of the newest file in raw/, or None if raw/ is empty."""
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
    """Return True when no build is published or the last full build started before the
    newest raw file.

    A failed build doesn't write the stamp, so the warehouse stays stale.
    """
    from sdp import transform
    stamp = transform.stamp_path()
    if not settings.warehouse_path.exists() or not stamp.exists():
        return True
    raw = _newest_raw_mtime()
    if raw is None:
        return False
    return stamp.stat().st_mtime < raw


def _build_dbt() -> int:
    """Run the dbt build. A thin wrapper so a test can replace it."""
    from sdp import transform
    return transform.main(["build"])


# ---------- the status snapshot ----------


def _expected_last_session(now_utc: dt.datetime) -> dt.date | None:
    """Return the newest XNYS session whose S3 flat file should exist now.

    Session D's file lands about 06:00 UTC on D+1, so before 06:30 UTC the day before
    yesterday is the safe answer.
    """
    through = now_utc.date() - dt.timedelta(days=1)
    if now_utc.time() < dt.time(6, 30):
        through -= dt.timedelta(days=1)
    days = backfill.sessions(through - dt.timedelta(days=12), through)
    return days[-1] if days else None


def _sessions_behind(last: dt.date | None, expected: dt.date | None) -> int | None:
    """Return the XNYS sessions between the last partition and the expected last session."""
    if last is None or expected is None:
        return None
    if last >= expected:
        return 0
    return len(backfill.sessions(last + dt.timedelta(days=1), expected))


def _read_last_pull() -> dict | None:
    """Return the last_pull block of the previous status file, or None."""
    p = _logs_dir() / "status.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8")).get("last_pull")
    except (OSError, ValueError):
        return None



def status_snapshot(now_utc: dt.datetime | None = None,
                    last_pull: dict | None = None) -> dict:
    """Describe every dataset, the pipeline, the warehouse and storage (see sdp.status)."""
    from sdp import status
    return status.snapshot(now_utc, last_pull)


RUNS_KEEP = 500
"""Lines kept in data/_logs/runs.jsonl, one per update."""


def _append_run(record: dict) -> None:
    """Append one run to runs.jsonl and keep the last RUNS_KEEP. An error only logs."""
    p = _logs_dir() / "runs.jsonl"
    try:
        lines = p.read_text(encoding="utf-8").splitlines() if p.exists() else []
        lines = (lines + [json.dumps(record)])[-RUNS_KEEP:]
        tmp = p.with_suffix(".jsonl.tmp")
        tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
        os.replace(tmp, p)
    except OSError as exc:
        log.warning("The run history was not written. %s", exc)


def _write_status(snap: dict) -> None:
    p = _logs_dir() / "status.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(snap, indent=2), encoding="utf-8")
    os.replace(tmp, p)


# ---------- the offline probe ----------

def _online() -> bool:
    """Return True when at least one vendor host resolves.

    launchd runs a missed tick on wake, often before the network is up. When only one
    host fails, its pulls run and fail, so the failure count shows the problem.
    """
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
    """Count the failed runs in a row.

    A clean run that pulled resets the count. A run that did not pull (offline) keeps it.
    """
    n = prev.get("failures_in_a_row")
    n = n if isinstance(n, int) else 0
    if failed:
        return n + 1
    return 0 if pulled else n


def _offline_since(prev: dict, offline: bool, now: dt.datetime) -> str | None:
    """Return the time of the first offline run in the current series, or None."""
    if not offline:
        return None
    since = prev.get("offline_since")
    return since if isinstance(since, str) else now.isoformat()


def _alert_message(*, failures: int, failed: bool, offline_since: str | None,
                   now: dt.datetime) -> str | None:
    """Return the text of the alert this run must send, or None."""
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
    """Show a macOS notification. An error only logs: an alert must not stop the update."""
    text = message.replace("\\", "\\\\").replace('"', '\\"')
    try:
        subprocess.run(
            ["osascript", "-e", f'display notification "{text}" with title "sdp"'],
            check=False, capture_output=True, timeout=10,
        )
    except Exception as exc:  # noqa: BLE001 -- a failed alert must not fail the run
        log.warning("The notification failed. %s: %s", type(exc).__name__, exc)


def _alert(message: str | None, prev: dict, today: str) -> str | None:
    """Send the message as a notification, at most one a day. Return the ISO date of the
    last alert."""
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
    """Delete backfill run logs older than LOG_KEEP_DAYS and rotate large launchd logs.

    An error only logs: pruning must not fail the run.
    """
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
                skip_current_state: bool, skip_event_streams: bool,
                skip_fund_data: bool = False) -> int:
    """Count the units of work, so the dashboard can draw a progress bar.

    One unit is a current-state pull or one session, plus one for the dbt step.
    """
    total = 0 if skip_current_state else len(CURRENT_STATE)
    if not skip_event_streams:
        for name, ds in EVENT_STREAMS.items():
            total += _pending_count(ds, _fill_end(name, today), max_sessions)
        total += len(_pending_month_ends(today))
    if not skip_fund_data:
        total += len(FUND_DATA)
    return total + 1  # The dbt step is always one unit, built or skipped.


def update(
    today: dt.date | None = None,
    *,
    max_sessions: int = DEFAULT_MAX_SESSIONS,
    force: bool = False,
    force_dbt: bool = False,
    skip_current_state: bool = False,
    skip_event_streams: bool = False,
    skip_fund_data: bool = False,
    progress: Callable[[dict], None] | None = None,
) -> dict:
    """Pull the datasets, build the dbt models when a new session landed, and write the
    status file.

    One writer at a time; the scheduler and the dashboard button both call this.
    progress receives a dict with a message and a done/total count for each step. When
    the vendor hosts don't resolve, the pulls are skipped and the exit code is 0.

    Raises:
        UpdateInProgress: when the lock is held.
    """
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

    started = time.monotonic()
    with _update_lock():
        wants_pull = not (skip_current_state and skip_event_streams and skip_fund_data)
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
                                    skip_event_streams=skip_event_streams,
                                    skip_fund_data=skip_fund_data)
                code = run(today, max_sessions=max_sessions,
                           skip_current_state=skip_current_state,
                           skip_event_streams=skip_event_streams,
                           skip_fund_data=skip_fund_data, force=force,
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
        _append_run({"time": now.isoformat(), "exit_code": code, "dbt": dbt,
                     "offline": offline, "seconds": round(time.monotonic() - started),
                     "problems": len(problems), "first_problem": problems[0] if problems else None})
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
    p.add_argument("--skip-fund-data", action="store_true",
                   help="Do not pull the borrow file, the ETF screeners, NAV histories "
                        "or holdings.")
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
            skip_fund_data=args.skip_fund_data,
        )
    except UpdateInProgress as exc:
        log.warning("%s", exc)
        return 0  # Another writer has it. This is not a failure.
    return snap["last_pull"]["exit_code"]


if __name__ == "__main__":
    sys.exit(main())

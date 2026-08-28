# src/sdp/daily.py
"""The daily driver, one command for a scheduler.

    python -m sdp.daily

Two jobs. (1) Pull the current-state datasets (splits, dividends); each pull
replaces the whole table, so a missed day costs nothing. (2) Fill the event
streams (day_aggs, tickers) from the day after the last partition, so a machine
that slept self-heals. Step 2 is capped so a long gap does not become a
backfill. Exits 1 on any failure.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import sys

from sdp import backfill, dal
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


def _pull_current_state(today: dt.date, *, force: bool = False) -> list[str]:
    """Pull each current-state dataset for today. Return the names that failed."""
    failed = []
    for dataset in CURRENT_STATE:
        try:
            path = ca.ingest(dataset, today, force=force)
            log.info("%s: %s", dataset, path)
        except Exception as exc:  # noqa: BLE001 -- one dataset must not stop the other
            failed.append(dataset)
            log.error("%s FAILED %s: %s", dataset, type(exc).__name__, exc)
    return failed


def _fill_event_stream(
    name: str,
    ds: dal.Dataset,
    today: dt.date,
    max_sessions: int,
) -> list[tuple[dt.date, str]]:
    """Publish every missing session up to today. Return the failures."""
    parts = dal.partitions(ds)
    if parts:
        start = parts[-1] + dt.timedelta(days=1)
    else:
        start = today - dt.timedelta(days=COLD_START_DAYS)
        log.warning("%s has no partition. The fill starts at %s. Run "
                    "python -m sdp.backfill for a full backfill.", name, start)

    if start > today:
        log.info("%s is up to date at %s.", name, parts[-1])
        return []

    pending = backfill.sessions(start, today)
    if not pending:
        log.info("%s: no XNYS session between %s and %s.", name, start, today)
        return []
    if len(pending) > max_sessions:
        log.warning("%s has %s sessions to fill and the limit is %s. The rest "
                    "closes over the runs of the next days.",
                    name, len(pending), max_sessions)

    return backfill.backfill(name, start, today, limit=max_sessions)


def run(
    today: dt.date | None = None,
    *,
    max_sessions: int = DEFAULT_MAX_SESSIONS,
    skip_current_state: bool = False,
    skip_event_streams: bool = False,
    force: bool = False,
) -> int:
    """Run the daily job. Return the exit code."""
    today = today or dt.datetime.now(dt.UTC).date()
    log.info("Daily run for %s.", today)

    problems: list[str] = []

    if not skip_current_state:
        failed = _pull_current_state(today, force=force)
        problems += [f"{name} pull failed" for name in failed]

    if not skip_event_streams:
        for name, ds in EVENT_STREAMS.items():
            failures = _fill_event_stream(name, ds, today, max_sessions)
            problems += [f"{name} {d}: {msg}" for d, msg in failures]

    print("\n" + dal.status())
    if problems:
        print(f"\n{len(problems)} problems:")
        for p in problems[:20]:
            print(f"  {p}")
        return 1
    print("\nThe daily run finished with no problem.")
    return 0


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
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Quiet httpx's one-INFO-line-per-request. Retries and failures still log
    # from the sdp loggers.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return run(
        args.date,
        max_sessions=args.max_sessions,
        skip_current_state=args.skip_current_state,
        skip_event_streams=args.skip_event_streams,
        force=args.force,
    )


if __name__ == "__main__":
    sys.exit(main())

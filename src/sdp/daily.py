# src/sdp/daily.py
"""The daily driver. One command for a scheduler to call.

    python -m sdp.daily

The command does two different jobs, because the two kinds of dataset need
different treatment.

1. It pulls the current-state datasets for today. These are `massive_splits` and
   `massive_dividends`. A missed day here is unrecoverable, so this step runs
   every day and it includes a weekend. An announcement made on a Friday evening
   is captured by the Saturday pull.

2. It fills the event streams up to today. These are `day_aggs` and `tickers`.
   The step starts at the day after the last published partition. The driver is
   therefore self-healing. A machine that was asleep, or a run that failed,
   recovers on the next run with no manual step.

Step 2 has a limit on the number of sessions, so that a long gap does not turn
one daily run into a backfill of several hours. A gap larger than the limit
closes over the runs of the next days. Use `sdp.backfill` directly for a real
backfill.

The command exits with 1 when any step fails, so that a wrapper can raise an
alert.
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

EVENT_STREAMS = {"day_aggs": dal.DAY_AGGS, "tickers": dal.TICKERS}

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


def _warn_if_the_record_has_a_gap(today: dt.date) -> None:
    """Log the size of any break in the pull_date series.

    A missed day is permanent. The splits endpoint has no declaration_date, so
    the pull_date partitions are the only record of when a split became
    knowable. A break must be visible in the log and not silent.
    """
    parts = dal.partitions(dal.SPLITS)
    if not parts:
        log.warning("massive_splits has no pull yet. The record starts today.")
        return
    missed = (today - parts[-1]).days - 1
    if missed > 0:
        log.warning(
            "The record has a gap of %s days, from %s to %s. Those days are "
            "permanent losses. No later pull can recover them.",
            missed, parts[-1] + dt.timedelta(days=1), today - dt.timedelta(days=1),
        )


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
        _warn_if_the_record_has_a_gap(today)
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
    return run(
        args.date,
        max_sessions=args.max_sessions,
        skip_current_state=args.skip_current_state,
        skip_event_streams=args.skip_event_streams,
        force=args.force,
    )


if __name__ == "__main__":
    sys.exit(main())

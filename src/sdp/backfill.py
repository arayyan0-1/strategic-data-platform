# src/sdp/backfill.py
"""Runner that loops over dates for the datasets that you can backfill.

    python -m sdp.backfill day_aggs 2021-08-01 2026-08-08
    python -m sdp.backfill tickers  2021-08-01 2026-08-08

The runner does one session at a time and catches the exception of each date.
One bad day must not stop the other 1,249 days. The run continues and reports
at the end.

The runner does not retry a failed date. ingest() ignores a partition that is
already published. A second run of the same command is therefore the retry, and
it continues from the point of failure.

Run the two targets as two processes. They share no state. A stall in one must
not block the other.

The runner is sequential on purpose. Concurrency needs a rate limit for each
host. It also mixes the log lines that make a failed date easy to find. The
work needs one night in both designs.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import sys
import time
from collections.abc import Callable
from pathlib import Path

import exchange_calendars as xcals

from sdp.config import settings
from sdp.ingest import massive_day_aggs, massive_short, massive_tickers

log = logging.getLogger("sdp.backfill")

# Only datasets that you can backfill. An event-stream partition holds the facts
# of one date, so you can rebuild it at any time. Splits and dividends are
# absent on purpose. See _NOT_BACKFILLABLE.
TARGETS: dict[str, Callable[..., Path | None]] = {
    "day_aggs": massive_day_aggs.ingest,
    "tickers": massive_tickers.ingest,
    "short_volume": massive_short.ingest_short_volume,
    "short_interest": massive_short.ingest_short_interest,
}

_NOT_BACKFILLABLE = {
    "splits": "massive_splits",
    "dividends": "massive_dividends",
    "massive_splits": "massive_splits",
    "massive_dividends": "massive_dividends",
}

_CAL = xcals.get_calendar("XNYS")


def sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    """Return the XNYS sessions from start to end. Both limits are inclusive."""
    if start > end:
        raise SystemExit(f"The start date {start} is after the end date {end}.")
    return [s.date() for s in _CAL.sessions_in_range(start.isoformat(), end.isoformat())]


def _run_log(target: str) -> Path:
    """Return the path of a new run log. Run logs are under data/, which git ignores."""
    stamp = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    path = settings.data_root / "_logs" / f"backfill_{target}_{stamp}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def backfill(
    target: str,
    start: dt.date,
    end: dt.date,
    *,
    force: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
) -> list[tuple[dt.date, str]]:
    """Ingest every XNYS session in the range. Return the list of failures."""
    if target in _NOT_BACKFILLABLE and target not in TARGETS:
        raise SystemExit(
            f"You cannot backfill {target} ({_NOT_BACKFILLABLE[target]}). This "
            f"dataset holds current state. The endpoint has no as_of parameter, "
            f"so there is no history to fetch. The pull of today is the oldest "
            f"snapshot that can exist. Run this command each day instead:\n"
            f"    python -m sdp.ingest.massive_corporate_actions {_NOT_BACKFILLABLE[target]}"
        )
    if target not in TARGETS:
        raise SystemExit(
            f"The target {target!r} is unknown. Use one of: {', '.join(TARGETS)}."
        )

    ingest = TARGETS[target]
    days = sessions(start, end)
    if limit:
        days = days[:limit]

    if dry_run:
        print(f"{target}: {len(days)} sessions, from {days[0]} to {days[-1]}")
        return []

    run_log = _run_log(target)
    log.info("%s: %s sessions from %s to %s. Log: %s",
             target, len(days), days[0], days[-1], run_log)

    failures: list[tuple[dt.date, str]] = []
    done = skipped = 0
    t0 = time.monotonic()

    with run_log.open("w", encoding="utf-8") as fh:
        for i, d in enumerate(days, 1):
            record: dict = {"date": d.isoformat(), "target": target}
            started = time.monotonic()
            try:
                result = ingest(d, force=force)
                # ingest() returns None for a date that is not a session. The
                # calendar already removed those dates. None here therefore
                # means that the module refused the date.
                if result is None:
                    skipped += 1
                    record["status"] = "skipped"
                else:
                    done += 1
                    record["status"] = "ok"
                    record["path"] = str(result)
            except KeyboardInterrupt:
                log.warning("Stopped at %s (%s of %s). Run the command again to continue.",
                            d, i, len(days))
                break
            except Exception as exc:  # noqa: BLE001 -- one bad date must not stop the run
                msg = f"{type(exc).__name__}: {exc}"
                failures.append((d, msg))
                record["status"] = "failed"
                record["error"] = msg
                log.error("%s %s FAILED %s", target, d, msg)
            record["seconds"] = round(time.monotonic() - started, 2)
            fh.write(json.dumps(record) + "\n")
            fh.flush()  # A run that stops must still leave a log that you can read.

            if i % 25 == 0 or i == len(days):
                rate = i / max(time.monotonic() - t0, 1e-9)
                remaining = (len(days) - i) / rate if rate else 0
                log.info("%s: %s of %s  ok=%s skipped=%s failed=%s  about %.0f min left",
                         target, i, len(days), done, skipped, len(failures), remaining / 60)

    elapsed = time.monotonic() - t0
    print(f"\n{target}: {done} published, {skipped} skipped, {len(failures)} failed "
          f"in {elapsed / 60:.1f} min")
    print(f"Run log: {run_log}")
    if failures:
        print("\nThese dates failed. Run the same command again to retry only these dates:")
        for d, msg in failures[:20]:
            print(f"  {d}  {msg}")
        if len(failures) > 20:
            print(f"  ... and {len(failures) - 20} more. See the run log.")
    return failures


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m sdp.backfill",
        description="Ingest every XNYS session in a range of dates.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("target", help=f"One of: {', '.join(TARGETS)}.")
    p.add_argument("start", type=dt.date.fromisoformat, help="First date, as YYYY-MM-DD.")
    p.add_argument("end", type=dt.date.fromisoformat, help="Last date, as YYYY-MM-DD.")
    p.add_argument("--force", action="store_true",
                   help="Download and publish the dates that already exist.")
    p.add_argument("--dry-run", action="store_true",
                   help="Count the sessions and then stop.")
    p.add_argument("--limit", type=int,
                   help="Stop after N sessions. Use this flag to test a range first.")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    failures = backfill(args.target, args.start, args.end,
                        force=args.force, dry_run=args.dry_run, limit=args.limit)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

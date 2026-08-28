# src/sdp/ingest/massive_short.py
"""Ingest of the two FINRA short datasets, both event streams keyed on 'date'.

massive_short_volume: daily off-exchange short volume, coverage from 2024-02-06.
massive_short_interest: short interest on a two-week cadence, partition 'date'
equals the settlement date, coverage from 2017-12-29.

total_volume is FINRA off-exchange volume only, much smaller than the
day-aggregate volume, so never divide one by the other.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path

import duckdb
import exchange_calendars as xcals

from sdp.config import settings
from sdp.ingest.rest import dump_ndjson

log = logging.getLogger(__name__)

SHORT_VOLUME = "massive_short_volume"
SHORT_INTEREST = "massive_short_interest"

_VOLUME_PATH = "/stocks/v1/short-volume"
_INTEREST_PATH = "/stocks/v1/short-interest"

# Measured on 2026-08-22 with a sort on the ascending date.
VOLUME_FLOOR = dt.date(2024, 2, 6)
INTEREST_FLOOR = dt.date(2017, 12, 29)

# The vendor writes this into days_to_cover when avg_daily_volume is 0. A
# sentinel, not 1000 days, so reading it as a number inflates the least liquid names.
DAYS_TO_COVER_SENTINEL = 999.99

_CAL = xcals.get_calendar("XNYS")

_PAGE_LIMIT = 50_000


class AuditFailure(RuntimeError):
    pass


def _one(sql: str) -> tuple:
    row = duckdb.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"The query returned no rows: {sql[:120]}")
    return row


def raw_path(dataset: str, d: dt.date) -> Path:
    return settings.raw_dir / dataset / f"date={d:%Y-%m-%d}" / "data.parquet"


def is_trading_day(d: dt.date) -> bool:
    return _CAL.is_session(d.isoformat())


# ---------- WRITE ----------

def build(dataset: str, vendor_file: Path, d: dt.date) -> Path:
    """Convert the vendor NDJSON to staged Parquet. The 'date' column is the
    partition; for short interest it is the settlement date, and the vendor
    'settlement_date' column stays, because raw/ records what was sent."""
    staged = settings.staging_dir / dataset / f"{d:%Y-%m-%d}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)

    # The short volume endpoint already gives a 'date' column with the same
    # value. Drop the vendor copy and write the partition column, so that the
    # file has one column with that name.
    exclude = "exclude (date)" if dataset == SHORT_VOLUME else ""

    duckdb.execute(f"""
        copy (
            select * {exclude}, date '{d:%Y-%m-%d}' as date
            from read_json('{vendor_file}', format = 'newline_delimited',
                           sample_size = -1)
            order by ticker
        ) to '{staged}' (format parquet, compression zstd)
    """)
    return staged


# ---------- AUDIT ----------

_VOLUME_MIN_ROWS, _VOLUME_MAX_ROWS = 3_000, 30_000
_INTEREST_MIN_ROWS, _INTEREST_MAX_ROWS = 3_000, 30_000


def audit_short_volume(staged: Path, d: dt.date) -> int:
    (n_rows, null_ticker, dupes, negative, short_gt_total,
     null_ratio, fractional) = _one(f"""
        with s as (select * from read_parquet('{staged}'))
        select
            (select count(*) from s),
            (select count(*) filter (ticker is null) from s),
            (select count(*) from (
                select ticker from s group by ticker having count(*) > 1)),
            (select count(*) filter (
                short_volume < 0 or total_volume < 0
                or short_volume is null or total_volume is null) from s),
            (select count(*) filter (short_volume > total_volume) from s),
            (select count(*) filter (short_volume_ratio is null) from s),
            (select count(*) filter (total_volume <> floor(total_volume)) from s)
    """)

    fatal = []
    if not _VOLUME_MIN_ROWS <= n_rows <= _VOLUME_MAX_ROWS:
        fatal.append(f"{n_rows} rows, outside the limits "
                     f"[{_VOLUME_MIN_ROWS}, {_VOLUME_MAX_ROWS}]")
    if null_ticker:
        fatal.append(f"{null_ticker} null tickers")
    if dupes:
        fatal.append(f"{dupes} duplicate tickers")
    if negative:
        fatal.append(f"{negative} rows with a null or negative volume")
    if short_gt_total:
        # This gives a ratio above 100 percent. It is a wrong number and not an
        # unusual one.
        fatal.append(f"{short_gt_total} rows with short_volume above total_volume")
    if fatal:
        raise AuditFailure(f"{SHORT_VOLUME} {d}: " + ". ".join(fatal) + ".")

    if null_ratio:
        log.warning("%s: %s rows have a null short_volume_ratio.", d, null_ratio)
    if fractional:
        # Known and documented. The aggregate volumes are fractional and the
        # per-venue values are clean integers.
        log.info("%s: %s rows have a fractional total_volume.", d, fractional)
    log.info("%s: %s short volume rows.", d, n_rows)
    return n_rows


def audit_short_interest(staged: Path, d: dt.date) -> int:
    (n_rows, null_ticker, null_date, dupes, negative,
     sentinel, zero_adv, wrong_date) = _one(f"""
        with s as (select * from read_parquet('{staged}'))
        select
            (select count(*) from s),
            (select count(*) filter (ticker is null) from s),
            (select count(*) filter (settlement_date is null) from s),
            (select count(*) from (
                select ticker from s group by ticker having count(*) > 1)),
            (select count(*) filter (
                short_interest is null or short_interest < 0) from s),
            (select count(*) filter (
                days_to_cover >= {DAYS_TO_COVER_SENTINEL}) from s),
            (select count(*) filter (
                avg_daily_volume is null or avg_daily_volume = 0) from s),
            (select count(*) filter (
                cast(settlement_date as date) <> date '{d:%Y-%m-%d}') from s)
    """)

    fatal = []
    if not _INTEREST_MIN_ROWS <= n_rows <= _INTEREST_MAX_ROWS:
        fatal.append(f"{n_rows} rows, outside the limits "
                     f"[{_INTEREST_MIN_ROWS}, {_INTEREST_MAX_ROWS}]")
    if null_ticker:
        fatal.append(f"{null_ticker} null tickers")
    if null_date:
        fatal.append(f"{null_date} null settlement dates")
    if dupes:
        fatal.append(f"{dupes} duplicate tickers")
    if negative:
        fatal.append(f"{negative} rows with a null or negative short_interest")
    if wrong_date:
        # The request named one settlement date. A different date in the answer
        # puts the rows in the wrong partition.
        fatal.append(f"{wrong_date} rows with a settlement_date that is not {d}")
    if fatal:
        raise AuditFailure(f"{SHORT_INTEREST} {d}: " + ". ".join(fatal) + ".")

    if sentinel:
        log.warning("%s: %s rows have days_to_cover at or above the sentinel "
                    "%s. Read those rows as unknown and not as a number.",
                    d, sentinel, DAYS_TO_COVER_SENTINEL)
    if zero_adv:
        log.info("%s: %s rows have a null or zero avg_daily_volume.", d, zero_adv)
    log.info("%s: %s short interest rows.", d, n_rows)
    return n_rows


# ---------- PUBLISH ----------

def _ingest(dataset: str, path: str, params: dict, d: dt.date, audit,
            floor: dt.date, *, force: bool = False) -> Path | None:
    if not is_trading_day(d):
        log.debug("%s is not an XNYS session. Skipped.", d)
        return None
    if d < floor:
        log.info("%s starts on %s. %s is before that date. Skipped.",
                 dataset, floor, d)
        return None

    dest = raw_path(dataset, d)
    if dest.exists() and not force:
        return dest

    vendor_file = dump_ndjson(dataset, path, params, d,
                              force=force, allow_empty=True, compress=True)
    if vendor_file is None:
        return None

    staged = build(dataset, vendor_file, d)
    audit(staged, d)

    dest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staged, dest)
    return dest


def ingest_short_volume(d: dt.date, *, force: bool = False) -> Path | None:
    return _ingest(
        SHORT_VOLUME, _VOLUME_PATH,
        {"date": d.isoformat(), "limit": _PAGE_LIMIT},
        d, audit_short_volume, VOLUME_FLOOR, force=force,
    )


def ingest_short_interest(d: dt.date, *, force: bool = False) -> Path | None:
    """Ingest one settlement date. Most sessions have none, so the endpoint
    returns nothing and this returns None, which is correct."""
    return _ingest(
        SHORT_INTEREST, _INTEREST_PATH,
        {"settlement_date": d.isoformat(), "limit": _PAGE_LIMIT},
        d, audit_short_interest, INTEREST_FLOOR, force=force,
    )


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    which = sys.argv[1]
    day = dt.date.fromisoformat(sys.argv[2])
    forced = "--force" in sys.argv
    if which == "short_volume":
        ingest_short_volume(day, force=forced)
    elif which == "short_interest":
        ingest_short_interest(day, force=forced)
    else:
        raise SystemExit("Use short_volume or short_interest.")

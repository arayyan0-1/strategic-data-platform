# src/sdp/ingest/massive_day_aggs.py
from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path

import boto3
import duckdb
import exchange_calendars as xcals
from botocore.config import Config

from sdp.config import settings
from sdp.ingest.rest import _temp_beside

log = logging.getLogger(__name__)

DATASET = "us_stocks_day_aggs"
S3_PREFIX = "us_stocks_sip/day_aggs_v1"
_CAL = xcals.get_calendar("XNYS")


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=settings.massive_s3_endpoint,
        aws_access_key_id=settings.massive_s3_access_key_id,
        aws_secret_access_key=settings.massive_s3_secret_access_key,
        config=Config(signature_version="s3v4"),
    )


def _s3_key(d: dt.date) -> str:
    return f"{S3_PREFIX}/{d:%Y}/{d:%m}/{d:%Y-%m-%d}.csv.gz"


def vendor_path(d: dt.date) -> Path:
    return settings.vendor_dir / DATASET / f"{d:%Y-%m-%d}.csv.gz"


def raw_path(d: dt.date) -> Path:
    return settings.raw_dir / DATASET / f"date={d:%Y-%m-%d}" / "data.parquet"


def is_trading_day(d: dt.date) -> bool:
    return _CAL.is_session(d.isoformat())


# ---------- WRITE ----------

def download(d: dt.date, *, force: bool = False) -> Path:
    dest = vendor_path(d)
    if dest.exists() and not force:
        log.info("The vendor file is already present: %s", dest)
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Unique for each call, for the reason given in rest._temp_beside.
    tmp = _temp_beside(dest)
    try:
        _s3().download_file(settings.massive_s3_bucket, _s3_key(d), str(tmp))
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)
    log.info("Downloaded %s", dest)
    return dest


def build(d: dt.date) -> Path:
    """Convert the vendor CSV to staged Parquet: set column types and sort. No
    business logic."""
    src = vendor_path(d)
    staged = settings.staging_dir / DATASET / f"{d:%Y-%m-%d}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)

    duckdb.execute(f"""
        copy (
            select
                ticker,
                date '{d:%Y-%m-%d}'                as date,
                open, high, low, close,
                volume,
                transactions,
                to_timestamp(window_start / 1e9)   as window_start_utc
            from read_csv('{src}', compression = 'gzip', header = true)
            order by ticker
        ) to '{staged}' (format parquet, compression zstd)
    """)
    return staged


# ---------- AUDIT ----------

class AuditFailure(RuntimeError):
    pass


def audit(staged: Path) -> None:
    row = duckdb.execute(f"""
        select
            count(*)                                          as n_rows,
            count(distinct ticker)                            as n_tickers,
            count(*) filter (ticker is null)                  as null_tickers,
            count(*) filter (high < low)                      as bad_hl,
            count(*) filter (high < open or high < close)     as bad_high,
            count(*) filter (low > open or low > close)       as bad_low,
            count(*) filter (volume < 0)                      as bad_volume,
            count(*) filter (open <= 0 or close <= 0)         as bad_price
        from read_parquet('{staged}')
    """).fetchone()

    if row is None:
        raise AuditFailure(f"{staged.name}: the audit query returned no row.")

    (n_rows, n_tickers, null_tickers, bad_hl,
     bad_high, bad_low, bad_volume, bad_price) = row

    problems = []
    if n_rows < 5_000:
        problems.append(f"{n_rows} rows. A full session must have more than 5000 rows")
    if n_rows != n_tickers:
        problems.append(f"{n_rows - n_tickers} duplicate tickers")
    for name, count in [
        ("null tickers", null_tickers), ("high < low", bad_hl),
        ("high below open/close", bad_high), ("low above open/close", bad_low),
        ("negative volume", bad_volume), ("non-positive price", bad_price),
    ]:
        if count:
            problems.append(f"{count} rows with {name}")

    if problems:
        raise AuditFailure(f"{staged.name}: " + ". ".join(problems) + ".")
    log.info("The audit passed. %s rows, %s tickers.", n_rows, n_tickers)


# ---------- PUBLISH ----------

def publish(d: dt.date, staged: Path) -> Path:
    dest = raw_path(d)
    dest.parent.mkdir(parents=True, exist_ok=True)
    # This move is atomic, because both paths are on one filesystem.
    os.replace(staged, dest)
    log.info("Published %s", dest)
    return dest


def ingest(d: dt.date, *, force: bool = False) -> Path | None:
    if not is_trading_day(d):
        log.info("%s is not an XNYS session. Skipped.", d)
        return None
    if raw_path(d).exists() and not force:
        log.info("The partition is already published: %s", raw_path(d))
        return raw_path(d)

    download(d, force=force)
    staged = build(d)
    audit(staged)
    return publish(d, staged)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ingest(dt.date.fromisoformat(sys.argv[1]), force="--force" in sys.argv)
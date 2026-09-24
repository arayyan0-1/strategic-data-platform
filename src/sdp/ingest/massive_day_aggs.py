# src/sdp/ingest/massive_day_aggs.py
from __future__ import annotations

import datetime as dt
import logging
import os
from pathlib import Path

import boto3
import duckdb
from botocore.config import Config

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import (
    AuditFailure,
    add_mode_flags,
    check_mode,
    is_session,
    keep_on_failure,
    one,
    publish,
    require_file,
)
from sdp.ingest.rest import _temp_beside

log = logging.getLogger(__name__)

DATASET = dal.DAY_AGGS.name
S3_PREFIX = "us_stocks_sip/day_aggs_v1"


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

def audit(staged: Path) -> None:
    (n_rows, n_tickers, null_tickers, bad_hl,
     bad_high, bad_low, bad_volume, bad_price) = one(f"""
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
    """)

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

def ingest(d: dt.date, *, refetch: bool = False, rebuild: bool = False) -> Path | None:
    """Publish one session. The default skips a published partition. refetch
    downloads the vendor file again. rebuild reads the vendor file on disk only."""
    check_mode(refetch, rebuild)
    if not is_session(d):
        log.info("%s is not an XNYS session. Skipped.", d)
        return None
    dest = dal.DAY_AGGS.partition_file(d)
    if dest.exists() and not (refetch or rebuild):
        log.info("The partition is already published: %s", dest)
        return dest

    with keep_on_failure(vendor_path(d) if refetch else None):
        if rebuild:
            require_file(vendor_path(d))
        else:
            download(d, force=refetch)
        staged = build(d)
        audit(staged)
    return publish(staged, dest)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.massive_day_aggs",
                                description="Publish the day aggregates of one session.")
    p.add_argument("date", type=dt.date.fromisoformat, help="The session, as YYYY-MM-DD.")
    add_mode_flags(p)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ingest(args.date, refetch=args.refetch, rebuild=args.rebuild)

# src/sdp/ingest/massive_ticker_details.py
"""Ticker details for the common stock of one session: shares outstanding, market cap,
the SIC industry code and the listing date, from /v3/reference/tickers/{ticker}?date=.

    python -m sdp.ingest.massive_ticker_details 2026-08-31
    python -m sdp.backfill ticker_details 2021-08-01 2026-08-31

The endpoint answers one ticker per request, so the dataset is monthly: one partition
for the last XNYS session of each month, for each common stock with a bar on that
session. A model takes the newest partition on or before its date. The vendor states
market_cap for the whole company (GOOG and GOOGL show the same value), so a model
multiplies share_class_shares_outstanding by the price of the share class.
"""
from __future__ import annotations

import datetime as dt
import gzip
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

import duckdb

from sdp import dal
from sdp.config import settings
from sdp.ingest import rest
from sdp.ingest.common import (
    XNYS,
    AuditFailure,
    add_mode_flags,
    check_mode,
    keep_on_failure,
    one,
    publish,
    require_file,
)

log = logging.getLogger(__name__)

DATASET = dal.TICKER_DETAILS.name
TYPES = ("CS",)
WORKERS = 8
MAX_MISSING = 0.05    # The share of requested tickers that may fail before the pull stops.
MIN_ROWS = 2_000
MIN_SIC = 0.8         # The share of rows with an industry code, below which a warning logs.


def is_month_end(d: dt.date) -> bool:
    """Return True when d is the last XNYS session of its month."""
    return XNYS.is_session(d.isoformat()) and \
        XNYS.next_session(d.isoformat()).date().month != d.month


def month_end_sessions(start: dt.date, end: dt.date) -> list[dt.date]:
    """Return the last XNYS session of each month, from start to end inclusive."""
    days = XNYS.sessions_in_range(start.isoformat(), end.isoformat())
    return [s.date() for s in days if is_month_end(s.date())]


def _vendor_file(d: dt.date) -> Path:
    return settings.vendor_dir / DATASET / f"{d:%Y-%m-%d}.ndjson.gz"


def tickers_for(d: dt.date) -> list[str]:
    """Return the tickers of TYPES that have a bar on the session. Both source
    partitions must be published."""
    names = dal.on_date(dal.TICKERS, d).filter(
        f"type in ({', '.join(repr(t) for t in TYPES)})").select("ticker")
    bars = dal.on_date(dal.DAY_AGGS, d).select("ticker")
    return sorted(r[0] for r in names.intersect(bars).fetchall())


def fetch(d: dt.date, *, force: bool = False) -> Path:
    """Request each ticker of the session and write one gzip NDJSON file to vendor/.
    A ticker that the vendor does not know (HTTP 404) is left out. Raise when more than
    MAX_MISSING of the tickers fail, so a bad pull does not publish."""
    dest = _vendor_file(d)
    if dest.exists() and not force:
        return dest
    tickers = tickers_for(d)
    if not tickers:
        raise FileNotFoundError(f"{DATASET} {d}: no {TYPES} ticker has a bar. Publish the "
                                f"tickers and day_aggs partitions of {d} first.")

    def one_ticker(client, t: str) -> tuple[str, dict | None, str | None]:
        try:
            body = rest._get(client, f"/v3/reference/tickers/{quote(t, safe='')}",
                             {"date": d.isoformat()})
        except RuntimeError as exc:
            return t, None, str(exc).splitlines()[0]
        return t, body.get("results") or None, None

    with rest._client() as client, ThreadPoolExecutor(WORKERS) as pool:
        results = list(pool.map(lambda t: one_ticker(client, t), tickers))

    missing = [(t, err or "no result") for t, res, err in results if res is None]
    if len(missing) > MAX_MISSING * len(tickers):
        raise RuntimeError(f"{DATASET} {d}: {len(missing)} of {len(tickers)} tickers failed. "
                           f"The first is {missing[0][0]}: {missing[0][1]}")
    if missing:
        log.info("%s %s: %s of %s tickers have no details.", DATASET, d, len(missing),
                 len(tickers))

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = rest._temp_beside(dest)
    with gzip.open(tmp, "wt", encoding="utf-8") as fh:
        for t, res, _ in results:
            if res is not None:
                fh.write(json.dumps({**res, "requested_ticker": t}) + "\n")
    os.replace(tmp, dest)
    log.info("Wrote %s records to %s", len(results) - len(missing), dest)
    return dest


# The columns that the models use. A field that the vendor leaves out of every record
# of a pull becomes a null column, so a pull with no employee counts still builds.
COLUMNS = {
    "ticker": "varchar", "name": "varchar", "type": "varchar", "active": "boolean",
    "cik": "varchar", "composite_figi": "varchar", "share_class_figi": "varchar",
    "market_cap": "double", "share_class_shares_outstanding": "double",
    "weighted_shares_outstanding": "double", "sic_code": "varchar",
    "sic_description": "varchar", "list_date": "varchar", "primary_exchange": "varchar",
    "total_employees": "double", "round_lot": "double", "currency_name": "varchar",
}


def build(vendor_file: Path, d: dt.date) -> Path:
    """Convert the vendor NDJSON to staged Parquet with a fixed set of columns. The staged
    name is unique to the process, so two backfills of one date do not share a file."""
    staged = settings.staging_dir / DATASET / f"{d:%Y-%m-%d}-{os.getpid()}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"""
            create temp table src as
            select * from read_json('{vendor_file}', format = 'newline_delimited',
                                    sample_size = -1, union_by_name = true)""")
        have = {r[0] for r in con.execute("describe src").fetchall()}
        cols = ",\n".join(
            f"try_cast({c} as {t}) as {c}" if c in have else f"cast(null as {t}) as {c}"
            for c, t in COLUMNS.items())
        con.execute(f"""
            copy (
                select {cols}, date '{d:%Y-%m-%d}' as date
                from src
                order by ticker
            ) to '{staged}' (format parquet, compression zstd)""")
    finally:
        con.close()
    return staged


def audit(staged: Path, d: dt.date) -> int:
    """Raise AuditFailure on a table that would give wrong numbers. Return the row count."""
    n, dupes, null_ticker, neg, with_sic = one(f"""
        select count(*),
               count(*) - count(distinct ticker),
               count(*) filter (ticker is null),
               count(*) filter (market_cap < 0 or share_class_shares_outstanding < 0),
               count(sic_code)
        from read_parquet('{staged}')""")
    fatal = []
    if n < MIN_ROWS:
        fatal.append(f"{n} rows. The minimum is {MIN_ROWS}")
    if dupes:
        fatal.append(f"{dupes} duplicate tickers")
    if null_ticker:
        fatal.append(f"{null_ticker} null tickers")
    if neg:
        fatal.append(f"{neg} rows with a negative market cap or share count")
    if fatal:
        raise AuditFailure(f"{DATASET} {d}: " + ". ".join(fatal) + ".")
    if with_sic < MIN_SIC * n:
        log.warning("%s %s: %s of %s rows have an industry code.", DATASET, d, with_sic, n)
    log.info("%s %s: %s rows, %s with an industry code.", DATASET, d, n, with_sic)
    return n


def ingest(d: dt.date, *, refetch: bool = False, rebuild: bool = False) -> Path | None:
    """Publish the details of one month-end session. Return None for any other date.
    The default skips a published partition. refetch downloads again. rebuild reads
    the vendor file on disk only."""
    check_mode(refetch, rebuild)
    if not is_month_end(d):
        log.debug("%s is not the last session of its month. Skipped.", d)
        return None
    dest = dal.TICKER_DETAILS.partition_file(d)
    if dest.exists() and not (refetch or rebuild):
        return dest
    with keep_on_failure(_vendor_file(d) if refetch else None):
        vendor_file = require_file(_vendor_file(d)) if rebuild else fetch(d, force=refetch)
        staged = build(vendor_file, d)
        audit(staged, d)
    return publish(staged, dest)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.massive_ticker_details",
                                description="Publish the ticker details of one month-end "
                                            "session.")
    p.add_argument("date", type=dt.date.fromisoformat, help="The session, as YYYY-MM-DD.")
    add_mode_flags(p)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    print(ingest(args.date, refetch=args.refetch, rebuild=args.rebuild))

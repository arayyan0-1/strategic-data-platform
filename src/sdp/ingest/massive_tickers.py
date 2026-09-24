# src/sdp/ingest/massive_tickers.py
from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import duckdb

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
    require_vendor_ndjson,
    vendor_ndjson,
)
from sdp.ingest.rest import dump_ndjson

log = logging.getLogger(__name__)

DATASET = dal.TICKERS.name
PATH = "/v3/reference/tickers"


# ---------- WRITE ----------

def build(vendor_file: Path, d: dt.date) -> Path:
    """Convert the vendor NDJSON to staged Parquet. No pull timestamp is added:
    now() would break content idempotency, and vendor/ mtime records the fetch."""
    staged = settings.staging_dir / DATASET / f"{d:%Y-%m-%d}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)

    # Cursor pagination can return one ticker twice, identical but for
    # last_updated_utc, when the vendor revises it mid-pull. A transport
    # artifact, not data, so keep the newest copy here (not in dbt) and log it.
    n_dupes = one(f"""
        select count(*) - count(distinct ticker)
        from read_json('{vendor_file}', format = 'newline_delimited',
                       sample_size = -1)
    """)[0]
    if n_dupes:
        log.warning("%s: the vendor returned %s duplicate rows. Keeping the copy "
                    "with the newest last_updated_utc for each ticker.", d, n_dupes)

    duckdb.execute(f"""
        copy (
            select * exclude (_copy)
            from (
                select
                    *,
                    date '{d:%Y-%m-%d}'  as date,
                    row_number() over (
                        partition by ticker
                        order by last_updated_utc desc
                    ) as _copy
                from read_json('{vendor_file}',
                               format = 'newline_delimited',
                               sample_size = -1)
            )
            where _copy = 1
            order by ticker
        ) to '{staged}' (format parquet, compression zstd)
    """)
    return staged


# ---------- AUDIT ----------

MIN_ROWS, MAX_ROWS = 5_000, 40_000
MIN_CS = 3_000


def audit(staged: Path, d: dt.date) -> int:
    (n_rows, null_ticker, null_type, null_exch, null_figi,
     n_cs, n_inactive) = one(f"""
        select count(*),
               count(*) filter (ticker is null),
               count(*) filter (type is null),
               count(*) filter (primary_exchange is null),
               count(*) filter (composite_figi is null),
               count(*) filter (type = 'CS'),
               count(*) filter (active = false)
        from read_parquet('{staged}')
    """)

    dupes = one(f"""
        select count(*) from (
            select ticker from read_parquet('{staged}')
            group by ticker having count(*) > 1
        )
    """)[0]

    fatal = []
    if not MIN_ROWS <= n_rows <= MAX_ROWS:
        fatal.append(f"{n_rows} rows, outside the limits [{MIN_ROWS}, {MAX_ROWS}]")
    if null_ticker:
        fatal.append(f"{null_ticker} null tickers")
    if dupes:
        fatal.append(f"{dupes} duplicate tickers")
    if n_cs < MIN_CS:
        fatal.append(f"{n_cs} common stock rows. The minimum is {MIN_CS}")
    if n_inactive:
        fatal.append(f"{n_inactive} inactive rows, but the request used active=true")
    if fatal:
        raise AuditFailure(f"{DATASET} {d}: " + ". ".join(fatal) + ".")

    if null_type:
        log.warning("%s: %s rows have a null type.", d, null_type)
    if null_exch:
        log.info("%s: %s rows have a null primary_exchange.", d, null_exch)
    if null_figi:
        log.info("%s: %s rows have a null composite_figi.", d, null_figi)
    log.info("%s: %s rows, %s CS.", d, n_rows, n_cs)
    return n_rows


def _audit_vs_previous(staged: Path, d: dt.date, n_rows: int) -> None:
    """Check that the universe size is stable against the newest published
    partition before d."""
    priors = [p for p in dal.partitions(dal.TICKERS) if p < d]
    if not priors:
        return
    prev_d = priors[-1]
    (prev_n,) = dal.on_date(dal.TICKERS, prev_d).aggregate("count(*)").fetchone() or (0,)

    if abs(n_rows - prev_n) > max(200, prev_n * 0.05):
        raise AuditFailure(
            f"{DATASET} {d}: {n_rows} rows against {prev_n} on {prev_d}. "
            f"The universe changed too much."
        )


# ---------- PUBLISH ----------

def ingest(d: dt.date, *, refetch: bool = False, rebuild: bool = False) -> Path | None:
    """Publish the universe of one session. The default skips a published
    partition. refetch downloads the vendor file again. rebuild reads the vendor
    file on disk only."""
    check_mode(refetch, rebuild)
    if not is_session(d):
        log.debug("%s is not an XNYS session. Skipped.", d)
        return None
    dest = dal.TICKERS.partition_file(d)
    if dest.exists() and not (refetch or rebuild):
        return dest

    with keep_on_failure(vendor_ndjson(DATASET, d) if refetch else None):
        if rebuild:
            vendor_file = require_vendor_ndjson(DATASET, d)
        else:
            vendor_file = dump_ndjson(
                DATASET, PATH,
                {"date": d.isoformat(), "active": "true", "market": "stocks", "limit": 1000},
                d, force=refetch, compress=True,
            )
        staged = build(vendor_file, d)
        n_rows = audit(staged, d)
        _audit_vs_previous(staged, d, n_rows)
    return publish(staged, dest)


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.massive_tickers",
                                description="Publish the ticker universe of one session.")
    p.add_argument("date", type=dt.date.fromisoformat, help="The session, as YYYY-MM-DD.")
    add_mode_flags(p)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    ingest(args.date, refetch=args.refetch, rebuild=args.rebuild)

"""Ingest IBKR's short-stock file: borrow fee and shares available per US symbol.

    python -m sdp.ingest.ibkr_borrow                  # pull if today's snapshot is due
    python -m sdp.ingest.ibkr_borrow --force          # pull now
    python -m sdp.ingest.ibkr_borrow --rebuild DATE   # build DATE again from vendor/

The file is a live snapshot that IBKR rewrites through the day and keeps no history, so
a missed session is gone. One snapshot per session is published: the first pull at or
after PULL_HOUR_ET, keyed on the date in the file's own header (US Eastern).
"""
from __future__ import annotations

import argparse
import datetime as dt
import ftplib
import gzip
import logging
import time
from pathlib import Path

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import (
    EASTERN,
    AuditFailure,
    eastern,
    is_session,
    one,
    publish,
    require_file,
    write_gzip,
)

log = logging.getLogger(__name__)

DATASET = dal.IBKR_BORROW
FTP_USER = "shortstock"  # IBKR's public account; it has no password.
FTP_FILE = "usa.txt"
PULL_HOUR_ET = 8  # Before the open, after the overnight reset of rates.
MIN_ROWS = 10_000
MAX_ROW_CHANGE = 0.15  # Warn when the symbol count moves more than this against the last.


def vendor_file(d: dt.date) -> Path:
    return settings.vendor_dir / DATASET.name / f"{d:%Y-%m-%d}.txt.gz"


def fetch(attempts: int = 3) -> bytes:
    """Download the US file over FTP."""
    last = ""
    for i in range(attempts):
        try:
            with ftplib.FTP(settings.ibkr_ftp_host, timeout=60) as ftp:
                ftp.login(FTP_USER, "")
                chunks: list[bytes] = []
                ftp.retrbinary(f"RETR {FTP_FILE}", chunks.append)
                return b"".join(chunks)
        except (OSError, ftplib.Error) as exc:
            last = f"{type(exc).__name__}: {exc}"
            if i + 1 < attempts:
                log.warning("IBKR FTP attempt %s of %s failed: %s", i + 1, attempts, last)
                time.sleep(5 * 2 ** i)
    raise RuntimeError(f"All {attempts} attempts on {settings.ibkr_ftp_host} failed. {last}")


def header(data: bytes) -> tuple[dt.date, dt.datetime, int]:
    """Return the snapshot date, its time (aware, Eastern) and the row count in #EOF."""
    lines = data.decode("utf-8", "replace").splitlines()
    if not lines or not lines[0].startswith("#BOF|"):
        raise AuditFailure(f"The file does not start with #BOF: {lines[:1]!r}")
    eof = next((ln for ln in reversed(lines) if ln.startswith("#EOF|")), None)
    if eof is None:
        raise AuditFailure("The file has no #EOF line, so it is truncated.")
    _, day, clock = lines[0].split("|")[:3]
    stamp = dt.datetime.strptime(f"{day} {clock}", "%Y.%m.%d %H:%M:%S").replace(tzinfo=EASTERN)
    return stamp.date(), stamp, int(eof.split("|")[1])


def build(src: Path, d: dt.date, snapshot_at: dt.datetime) -> Path:
    """Convert the vendor file to staged Parquet."""
    staged = settings.staging_dir / DATASET.name / f"{d:%Y-%m-%d}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)
    cols = ", ".join(f"'{c}': 'varchar'" for c in
                     ("sym", "cur", "name", "con", "isin", "rebate", "fee", "avail", "figi",
                      "trailing"))
    utc = snapshot_at.astimezone(dt.UTC).strftime("%Y-%m-%d %H:%M:%S")
    one(f"""
        copy (
            select date '{d:%Y-%m-%d}'                               as date,
                   timestamptz '{utc}+00'                            as snapshot_at,
                   sym                                               as symbol,
                   replace(sym, ' ', '.')                            as ticker,
                   nullif(cur, '')                                   as currency,
                   replace(replace(replace(replace(name, '&amp;', '&'), '&quot;', '"'),
                           '&#39;', ''''), '&apos;', '''')           as name,
                   try_cast(con as bigint)                           as conid,
                   case when isin like 'X%' or isin = '' then null else isin end as isin,
                   try_cast(rebate as double)                        as rebate_rate,
                   try_cast(fee as double)                           as fee_rate,
                   try_cast(replace(avail, '>', '') as bigint)       as available,
                   avail like '>%'                                   as available_capped,
                   nullif(figi, '')                                  as composite_figi
            from read_csv('{src}', delim = '|', header = false, skip = 2, quote = '',
                          escape = '', columns = {{{cols}}}, null_padding = true,
                          strict_mode = false, auto_detect = false, compression = 'gzip')
            where sym not like '#%'
            order by symbol
        ) to '{staged}' (format parquet, compression zstd)""")
    return staged


def audit(staged: Path, d: dt.date, expected_rows: int) -> int:
    """Raise AuditFailure on a snapshot that would give wrong numbers; return the rows."""
    n, null_sym, dup_sym, neg_fee, null_fee = one(f"""
        select count(*), count(*) filter (where symbol is null),
               count(*) - count(distinct symbol),
               count(*) filter (where fee_rate < 0), count(*) filter (where fee_rate is null)
        from read_parquet('{staged}')""")
    fatal = []
    if n != expected_rows:
        fatal.append(f"{n} rows, but #EOF says {expected_rows}")
    if n < MIN_ROWS:
        fatal.append(f"{n} rows. The minimum is {MIN_ROWS}")
    if null_sym:
        fatal.append(f"{null_sym} rows with no symbol")
    if dup_sym:
        fatal.append(f"{dup_sym} duplicate symbols")
    if neg_fee:
        fatal.append(f"{neg_fee} negative fee rates")
    if fatal:
        raise AuditFailure(f"{DATASET.name} {d}: " + ". ".join(fatal) + ".")
    if null_fee:
        log.info("%s %s: %s symbols have no fee rate.", DATASET.name, d, null_fee)
    priors = [p for p in dal.partitions(DATASET) if p < d]
    if priors:
        (prev,) = one(f"select count(*) from read_parquet('{DATASET.partition_file(priors[-1])}')")
        if prev and abs(n - prev) > prev * MAX_ROW_CHANGE:
            log.warning("%s %s: %s symbols against %s on %s.", DATASET.name, d, n, prev,
                        priors[-1])
    return n


def _publish_from(src: Path) -> Path:
    d, stamp, expected = header(gzip.decompress(src.read_bytes()))
    staged = build(src, d, stamp)
    try:
        n = audit(staged, d, expected)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    path = publish(staged, DATASET.partition_file(d))
    log.info("%s %s: %s symbols, snapshot at %s ET.", DATASET.name, d, n, f"{stamp:%H:%M}")
    return path


def due(now: dt.datetime | None = None) -> bool:
    """Return True when today's snapshot is due: a session, after PULL_HOUR_ET, unpublished."""
    et = eastern(now)
    return (is_session(et.date()) and et.hour >= PULL_HOUR_ET
            and not DATASET.partition_file(et.date()).exists())


def ingest(now: dt.datetime | None = None, *, force: bool = False) -> Path | None:
    """Publish today's snapshot when it is due. force skips the clock, never a partition.

    Return the partition, or None when nothing was due or the file is not of a session.
    """
    if not (force or due(now)):
        return None
    data = fetch()
    d, _, _ = header(data)
    if not is_session(d):
        log.info("%s: the file is of %s, not a session. Skipped.", DATASET.name, d)
        return None
    if DATASET.partition_file(d).exists():
        log.info("%s already has the snapshot of %s.", DATASET.name, d)
        return DATASET.partition_file(d)
    return _publish_from(write_gzip(data, vendor_file(d)))


def rebuild(d: dt.date) -> Path:
    """Build one date again from its vendor file, with no network."""
    return _publish_from(require_file(vendor_file(d)))


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.ibkr_borrow",
                                description="Publish the IBKR short-stock snapshot.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true", help="Pull now, whatever the time.")
    mode.add_argument("--rebuild", type=dt.date.fromisoformat, metavar="DATE",
                      help="Build DATE again from its vendor file. Do not download.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(rebuild(args.rebuild) if args.rebuild else ingest(force=args.force))


if __name__ == "__main__":
    main()

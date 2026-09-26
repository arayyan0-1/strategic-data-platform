# src/sdp/ingest/french.py
"""The Fama-French daily factors from the Kenneth French Data Library.

    python -m sdp.ingest.french             # pull when the table is a week old
    python -m sdp.ingest.french --force     # pull now
    python -m sdp.ingest.french --rebuild   # build again from the newest vendor pull

Each file states the whole history, and the library revises it, so the table is
current state: one file in raw/, replaced by each pull. vendor/ keeps the zip files
of each pull. The files give daily returns in percent. The table holds fractions.
The library updates about once a month, so a pull is due only after MAX_AGE_DAYS.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import logging
import os
import re
import time
import zipfile
from pathlib import Path

import duckdb
import httpx
import numpy as np

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import AuditFailure, one, publish
from sdp.ingest.rest import _temp_beside

log = logging.getLogger(__name__)

DATASET = dal.FRENCH

# Each file and the columns it gives after the date, in order.
FILES = {
    "F-F_Research_Data_5_Factors_2x3_daily_CSV.zip": ("mkt_rf", "smb", "hml", "rmw", "cma", "rf"),
    "F-F_Momentum_Factor_daily_CSV.zip": ("mom",),
}
FACTORS = ("mkt_rf", "smb", "hml", "rmw", "cma", "rf")

MAX_AGE_DAYS = 7
MIN_ROWS = 10_000     # The daily five-factor file starts in 1963.
MAX_ABS = 0.5         # No daily factor return reaches 50 percent.
STALE_DAYS = 200      # The library lags about two months. A longer lag is unusual.


def _pull_dir(pull_date: dt.date) -> Path:
    return settings.vendor_dir / DATASET.name / f"{pull_date:%Y-%m-%d}"


def vendor_pulls() -> list[tuple[dt.date, Path]]:
    """Return the complete vendor pulls, oldest first."""
    root = settings.vendor_dir / DATASET.name
    out = []
    for p in sorted(root.glob("????-??-??")) if root.exists() else []:
        try:
            d = dt.date.fromisoformat(p.name)
        except ValueError:
            continue
        if all((p / name).exists() for name in FILES):
            out.append((d, p))
    return out


def _download(name: str, attempts: int = 4) -> bytes:
    """Return the bytes of one library file. A transport error or HTTP 5xx causes a
    retry with backoff. The client sends no credentials."""
    url = settings.french_base_url + name
    last = ""
    with httpx.Client(timeout=httpx.Timeout(60.0, connect=10.0), follow_redirects=True,
                      headers={"User-Agent": "Mozilla/5.0"}) as client:
        for i in range(attempts):
            try:
                resp = client.get(url)
            except httpx.TransportError as exc:
                last = type(exc).__name__
            else:
                if resp.status_code < 500 and resp.is_error:
                    raise RuntimeError(f"HTTP {resp.status_code} on {url}")
                if not resp.is_error:
                    return resp.content
                last = f"HTTP {resp.status_code}"
            if i + 1 < attempts:
                log.warning("Attempt %s of %s on %s failed with %s.", i + 1, attempts, url, last)
                time.sleep(2 ** i)
    raise RuntimeError(f"All {attempts} attempts on {url} failed. The last error was {last}.")


def fetch(pull_date: dt.date) -> Path:
    """Download each file into the vendor directory of the pull date."""
    d = _pull_dir(pull_date)
    d.mkdir(parents=True, exist_ok=True)
    for name in FILES:
        data = _download(name)
        tmp = _temp_beside(d / name)
        tmp.write_bytes(data)
        os.replace(tmp, d / name)
        log.info("Wrote %s bytes to %s", len(data), d / name)
    return d


def _parse(data: bytes, cols: tuple[str, ...]) -> tuple[dict[str, list[float]], str | None]:
    """Return the daily rows of one zip file by date, as fractions, and the CRSP month
    that built the file. A row is a line with an 8-digit date and one value per column."""
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        text = z.read(z.namelist()[0]).decode("latin-1")
    crsp = re.search(r"(\d{6}) CRSP", text)
    rows: dict[str, list[float]] = {}
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(",")]
        if len(parts) == len(cols) + 1 and parts[0].isdigit() and len(parts[0]) == 8:
            if parts[0] in rows:
                raise AuditFailure(f"The file repeats the date {parts[0]}.")
            rows[parts[0]] = [float(v) / 100 for v in parts[1:]]
    return rows, crsp.group(1) if crsp else None


def build(pull: Path, pull_date: dt.date) -> Path:
    """Build the table from one vendor pull into _staging/. Return the staged file."""
    (five_name, five_cols), (mom_name, mom_cols) = FILES.items()
    five, crsp = _parse((pull / five_name).read_bytes(), five_cols)
    mom, _ = _parse((pull / mom_name).read_bytes(), mom_cols)
    keys = sorted(five)
    arrays = {"date": np.array([f"{k[:4]}-{k[4:6]}-{k[6:]}" for k in keys])}
    for j, col in enumerate(five_cols):
        arrays[col] = np.array([five[k][j] for k in keys])
    arrays["mom"] = np.array([mom[k][0] if k in mom else np.nan for k in keys])
    staged = settings.staging_dir / f"{DATASET.name}-{os.getpid()}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)
    french_rows = arrays  # noqa: F841 -- DuckDB reads the local variable by its name.
    con = duckdb.connect()
    try:
        con.execute(f"""
            copy (
                select cast(date as date) as date,
                       {", ".join(f"round({c}, 8) as {c}" for c in FACTORS)},
                       case when isnan(mom) then null else round(mom, 8) end as mom,
                       {f"'{crsp}'" if crsp else "null"}::varchar as crsp_month,
                       date '{pull_date}' as vendor_pull_date
                from french_rows
                order by date
            ) to '{staged}' (format parquet)""")
    finally:
        con.close()
    return staged


def audit(staged: Path, pull_date: dt.date) -> int:
    """Raise AuditFailure on a table that would give wrong numbers. Return the row count."""
    n, dups, nulls, worst, last = one(f"""
        select count(*), count(*) - count(distinct date),
               count(*) filter ({" or ".join(f"{c} is null" for c in FACTORS)}),
               greatest({", ".join(f"max(abs({c}))" for c in (*FACTORS, "mom"))}),
               max(date)
        from read_parquet('{staged}')""")
    if n < MIN_ROWS:
        raise AuditFailure(f"The factor table has {n} rows. The minimum is {MIN_ROWS}.")
    if dups:
        raise AuditFailure(f"The factor table has {dups} duplicate dates.")
    if nulls:
        raise AuditFailure(f"The factor table has {nulls} rows with a null factor.")
    if worst is None or worst >= MAX_ABS:
        raise AuditFailure(f"A daily factor return is {worst}. The limit is {MAX_ABS}.")
    if DATASET.table_file.exists():
        (before,) = one(f"select count(*) from read_parquet('{DATASET.table_file}')")
        if n < before:
            raise AuditFailure(f"The pull has {n} rows and the published table has {before}. "
                               f"The library does not remove history.")
    if (pull_date - last).days > STALE_DAYS:
        log.warning("The last factor date is %s, %s days before the pull.",
                    last, (pull_date - last).days)
    return n


def published_pull() -> dt.date | None:
    """Return the vendor pull date of the published table, or None."""
    if not DATASET.table_file.exists():
        return None
    (d,) = one(f"select max(vendor_pull_date) from read_parquet('{DATASET.table_file}')")
    return d


def ingest(pull_date: dt.date | None = None, *, force: bool = False,
           rebuild: bool = False) -> Path:
    """Pull the factor files and publish the table. Without force, do nothing while the
    published table is younger than MAX_AGE_DAYS. rebuild reads the newest vendor pull
    and does not download."""
    pull_date = pull_date or dt.datetime.now(dt.UTC).date()
    last = published_pull()
    if not (force or rebuild) and last and (pull_date - last).days < MAX_AGE_DAYS:
        log.info("%s is from the pull of %s. The next pull is due %s days later.",
                 DATASET.name, last, MAX_AGE_DAYS)
        return DATASET.table_file
    if rebuild:
        pulls = vendor_pulls()
        if not pulls:
            raise FileNotFoundError(f"{DATASET.name} has no vendor pull to rebuild from.")
        pull_date, pull = pulls[-1]
    else:
        pull = fetch(pull_date)
    staged = build(pull, pull_date)
    try:
        n = audit(staged, pull_date)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    path = publish(staged, DATASET.table_file)
    log.info("Published %s: %s rows from the pull of %s.", path, n, pull_date)
    return path


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.french",
                                description="Pull the Fama-French daily factors.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true", help="Pull now, whatever the age.")
    mode.add_argument("--rebuild", action="store_true",
                      help="Build again from the newest vendor pull. Do not download.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(ingest(force=args.force, rebuild=args.rebuild))


if __name__ == "__main__":
    main()

# src/sdp/ingest/qfactors.py
"""The daily q5 factors of Hou, Xue and Zhang, from global-q.org.

    python -m sdp.ingest.qfactors             # pull when the table is a month old
    python -m sdp.ingest.qfactors --force     # pull now
    python -m sdp.ingest.qfactors --rebuild   # build again from the newest vendor pull

The factors are the market excess return, size (me), investment (ia), profitability
(roe) and expected growth (eg), with the risk-free rate. The file states the whole
history and the authors publish a new vintage about once a year, so the table is
current state: one file in raw/, replaced by each pull. The page links the file of the
latest vintage, and its name carries the year. The file gives percent. The table holds
fractions.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import re
from pathlib import Path
from urllib.parse import urljoin

import duckdb

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import AuditFailure, get_public, one, publish
from sdp.ingest.rest import _temp_beside

log = logging.getLogger(__name__)

DATASET = dal.QFACTORS

COLUMNS = ("r_f", "r_mkt", "r_me", "r_ia", "r_roe", "r_eg")
LINK = re.compile(r'href="([^"]*q5_factors_daily_(\d{4})\.csv)"')

MAX_AGE_DAYS = 30
MIN_ROWS = 10_000     # The daily file starts in 1967.
MAX_ABS = 0.5         # No daily factor return reaches 50 percent.


def _pull_dir(pull_date: dt.date) -> Path:
    return settings.vendor_dir / DATASET.name / f"{pull_date:%Y-%m-%d}"


def vendor_pulls() -> list[tuple[dt.date, Path]]:
    """Return the vendor pulls that hold a factor file, oldest first."""
    root = settings.vendor_dir / DATASET.name
    out = []
    for p in sorted(root.glob("????-??-??")) if root.exists() else []:
        try:
            d = dt.date.fromisoformat(p.name)
        except ValueError:
            continue
        files = sorted(p.glob("q5_factors_daily_*.csv"))
        if files:
            out.append((d, files[-1]))
    return out


def _download(url: str, attempts: int = 4) -> bytes:
    return get_public(url, attempts)


def latest_link(page: bytes) -> tuple[str, int]:
    """Return the URL and the vintage year of the newest daily factor file on the page."""
    found = [(m.group(1), int(m.group(2))) for m in LINK.finditer(page.decode("utf-8", "replace"))]
    if not found:
        raise AuditFailure("The factor page links no daily q5 factor file.")
    href, year = max(found, key=lambda x: x[1])
    return urljoin(settings.qfactors_page_url, href), year


def fetch(pull_date: dt.date) -> Path:
    """Download the newest daily factor file into the vendor directory of the pull date."""
    url, year = latest_link(_download(settings.qfactors_page_url))
    d = _pull_dir(pull_date)
    d.mkdir(parents=True, exist_ok=True)
    dest = d / f"q5_factors_daily_{year}.csv"
    data = _download(url)
    tmp = _temp_beside(dest)
    tmp.write_bytes(data)
    os.replace(tmp, dest)
    log.info("Wrote %s bytes to %s", len(data), dest)
    return dest


def build(src: Path, pull_date: dt.date) -> Path:
    """Build the table from one vendor file into _staging/. Return the staged file."""
    vintage = re.search(r"_(\d{4})\.csv$", src.name).group(1)
    staged = settings.staging_dir / f"{DATASET.name}-{os.getpid()}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(f"""
            copy (
                select cast(date as date) as date,
                       {", ".join(f"round({c} / 100, 8) as {c}" for c in COLUMNS)},
                       '{vintage}' as vintage,
                       date '{pull_date}' as vendor_pull_date
                from read_csv('{src}', header = true, normalize_names = true,
                              columns = {{'date': 'varchar',
                                          {", ".join(f"'{c}': 'double'" for c in COLUMNS)}}})
                order by date
            ) to '{staged}' (format parquet)""")
    finally:
        con.close()
    return staged


def audit(staged: Path) -> int:
    """Raise AuditFailure on a table that would give wrong numbers. Return the row count."""
    n, dups, nulls, worst = one(f"""
        select count(*), count(*) - count(distinct date),
               count(*) filter ({" or ".join(f"{c} is null" for c in COLUMNS)}),
               greatest({", ".join(f"max(abs({c}))" for c in COLUMNS)})
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
                               f"A new vintage does not remove history.")
    return n


def published_pull() -> dt.date | None:
    """Return the vendor pull date of the published table, or None."""
    if not DATASET.table_file.exists():
        return None
    (d,) = one(f"select max(vendor_pull_date) from read_parquet('{DATASET.table_file}')")
    return d


def ingest(pull_date: dt.date | None = None, *, force: bool = False,
           rebuild: bool = False) -> Path:
    """Pull the newest factor file and publish the table. Without force, do nothing while
    the published table is younger than MAX_AGE_DAYS. rebuild reads the newest vendor
    pull and does not download."""
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
        pull_date, src = pulls[-1]
    else:
        src = fetch(pull_date)
    staged = build(src, pull_date)
    try:
        n = audit(staged)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    path = publish(staged, DATASET.table_file)
    log.info("Published %s: %s rows from the pull of %s.", path, n, pull_date)
    return path


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.qfactors",
                                description="Pull the daily q5 factors.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true", help="Pull now, whatever the age.")
    mode.add_argument("--rebuild", action="store_true",
                      help="Build again from the newest vendor pull. Do not download.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(ingest(force=args.force, rebuild=args.rebuild))


if __name__ == "__main__":
    main()

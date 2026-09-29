# src/sdp/ingest/fred.py
"""Rates, credit spreads, volatility and financial conditions from FRED.

    python -m sdp.ingest.fred             # pull when the table is from an earlier day
    python -m sdp.ingest.fred --force     # pull now
    python -m sdp.ingest.fred --rebuild   # build again from the vendor pulls

Each pull downloads one CSV per series, with the whole history that FRED serves. FRED
revises some values, so the table is current state: one file in raw/, replaced by each
pull. FRED serves only the last three years of the ICE BofA spreads, so the table is
the fold of every kept pull: each (series, date) takes the value of the newest pull
that has it, and a date that left the window keeps its last value. vendor/ keeps each
pull, gzipped. Pulls older than KEEP_DAYS are thinned to one per ISO week, which still
covers every date of a three-year window.

The values keep the units of FRED: percent for rates and spreads, index points for the
volatility, dollar and stress indices.
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import logging
import os
import shutil
from pathlib import Path

import duckdb

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import AuditFailure, get_public, one, publish
from sdp.ingest.rest import _temp_beside

log = logging.getLogger(__name__)

DATASET = dal.FRED

# The series and what each holds. The id is the FRED series id.
SERIES = {
    "DTB4WK": "4-week Treasury bill, secondary market, discount basis, percent",
    "DTB3": "3-month Treasury bill, secondary market, discount basis, percent",
    "DGS1MO": "1-month Treasury, constant maturity, percent",
    "DGS3MO": "3-month Treasury, constant maturity, percent",
    "DGS6MO": "6-month Treasury, constant maturity, percent",
    "DGS1": "1-year Treasury, constant maturity, percent",
    "DGS2": "2-year Treasury, constant maturity, percent",
    "DGS3": "3-year Treasury, constant maturity, percent",
    "DGS5": "5-year Treasury, constant maturity, percent",
    "DGS7": "7-year Treasury, constant maturity, percent",
    "DGS10": "10-year Treasury, constant maturity, percent",
    "DGS20": "20-year Treasury, constant maturity, percent",
    "DGS30": "30-year Treasury, constant maturity, percent",
    "DFII5": "5-year TIPS, constant maturity, real yield, percent",
    "DFII7": "7-year TIPS, constant maturity, real yield, percent",
    "DFII10": "10-year TIPS, constant maturity, real yield, percent",
    "DFII20": "20-year TIPS, constant maturity, real yield, percent",
    "DFII30": "30-year TIPS, constant maturity, real yield, percent",
    "T5YIE": "5-year breakeven inflation, percent",
    "T10YIE": "10-year breakeven inflation, percent",
    "T5YIFR": "5-year, 5-year forward inflation expectation, percent",
    "DFF": "Effective federal funds rate, percent",
    "SOFR": "Secured overnight financing rate, percent",
    "BAMLH0A0HYM2": "ICE BofA US high yield option-adjusted spread, percent",
    "BAMLC0A0CM": "ICE BofA US corporate option-adjusted spread, percent",
    "VIXCLS": "CBOE VIX, close, index points",
    "VXVCLS": "CBOE 3-month VIX (VIX3M), close, index points",
    "DTWEXBGS": "Nominal broad US dollar index, goods and services",
    "NFCI": "Chicago Fed national financial conditions index, weekly",
    "STLFSI4": "St. Louis Fed financial stress index, weekly",
}

MAX_AGE_DAYS = 1
KEEP_DAYS = 30
MIN_VALUES = 500      # The shortest series (the ICE spreads) has about 750 values.
MAX_ABS = 1000.0      # No value in these units reaches 1,000.
STALE_DAYS = 21       # A weekly series is about 10 days behind. Longer is unusual.


def _pull_dir(pull_date: dt.date) -> Path:
    return settings.vendor_dir / DATASET.name / f"{pull_date:%Y-%m-%d}"


def _file(pull: Path, series: str) -> Path:
    return pull / f"{series}.csv.gz"


def vendor_pulls() -> list[tuple[dt.date, Path]]:
    """Return the vendor pulls that hold a file for one or more series, oldest first. A
    pull from before a series joined SERIES has no file for it and still counts, so the
    fold keeps the dates that only the older pulls hold."""
    root = settings.vendor_dir / DATASET.name
    out = []
    for p in sorted(root.glob("????-??-??")) if root.exists() else []:
        try:
            d = dt.date.fromisoformat(p.name)
        except ValueError:
            continue
        if any(_file(p, s).exists() for s in SERIES):
            out.append((d, p))
    return out


def _download(series: str, attempts: int = 4) -> bytes:
    """Return the CSV of one series: a header line, then one date and value per line."""
    return get_public(f"{settings.fred_csv_url}?id={series}", attempts, user_agent=None)


def fetch(pull_date: dt.date) -> Path:
    """Download each series into the vendor directory of the pull date, gzipped."""
    d = _pull_dir(pull_date)
    d.mkdir(parents=True, exist_ok=True)
    for series in SERIES:
        data = _download(series)
        if not data.startswith(b"observation_date,") and not data.startswith(b"DATE,"):
            raise AuditFailure(f"FRED did not return a CSV for {series}: {data[:80]!r}")
        tmp = _temp_beside(_file(d, series))
        tmp.write_bytes(gzip.compress(data))
        os.replace(tmp, _file(d, series))
    log.info("Wrote %s series to %s", len(SERIES), d)
    return d


def build(pulls: list[tuple[dt.date, Path]]) -> Path:
    """Fold the vendor pulls into one table in _staging/. Return the staged file."""
    files = ", ".join(f"'{_file(p, s)}'" for _, p in pulls for s in SERIES
                      if _file(p, s).exists())
    staged = settings.staging_dir / f"{DATASET.name}-{os.getpid()}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        # Each file has two columns: the date and the series id as the header of the
        # value. An empty value is a day with no observation.
        con.execute(f"""
            create temp table raw as
            select regexp_extract(filename, '([A-Z0-9]+)\\.csv\\.gz$', 1)           as series_id,
                   cast(regexp_extract(filename, '(\\d{{4}}-\\d{{2}}-\\d{{2}})/[^/]+$', 1)
                        as date)                                               as vendor_pull_date,
                   cast(column0 as date)                                       as date,
                   try_cast(nullif(trim(column1), '') as double)               as value
            from read_csv([{files}], header = true, columns = {{
                     'column0': 'varchar', 'column1': 'varchar'}},
                 filename = true, compression = 'gzip')""")
        con.execute(f"""
            copy (
                select series_id, date, arg_max(value, vendor_pull_date) as value,
                       max(vendor_pull_date) as vendor_pull_date
                from raw
                group by series_id, date
                order by series_id, date
            ) to '{staged}' (format parquet)""")
    finally:
        con.close()
    return staged


def audit(staged: Path, pull_date: dt.date) -> int:
    """Raise AuditFailure on a table that would give wrong numbers. Return the row count."""
    n, dups, worst = one(f"""
        select count(*), count(*) - count(distinct (series_id, date)), max(abs(value))
        from read_parquet('{staged}')""")
    if dups:
        raise AuditFailure(f"The table has {dups} duplicate (series, date) rows.")
    if worst is not None and worst >= MAX_ABS:
        raise AuditFailure(f"A value is {worst}. The limit is {MAX_ABS}.")
    counts = dict(duckdb.execute(f"""
        select series_id, count(value) from read_parquet('{staged}') group by 1""").fetchall())
    for series in SERIES:
        if counts.get(series, 0) < MIN_VALUES:
            raise AuditFailure(f"{series} has {counts.get(series, 0)} values. "
                               f"The minimum is {MIN_VALUES}.")
    for series, last in duckdb.execute(f"""
            select series_id, max(date) filter (where value is not null)
            from read_parquet('{staged}') group by 1""").fetchall():
        if (pull_date - last).days > STALE_DAYS:
            log.warning("%s ends on %s, %s days before the pull.", series, last,
                        (pull_date - last).days)
    if DATASET.table_file.exists():
        (before,) = one(f"select count(*) from read_parquet('{DATASET.table_file}')")
        if n < before:
            raise AuditFailure(f"The table has {n} rows and the published table has "
                               f"{before}. A fold of the pulls does not lose a date.")
    return n


def published_pull() -> dt.date | None:
    """Return the newest vendor pull date in the published table, or None."""
    if not DATASET.table_file.exists():
        return None
    (d,) = one(f"select max(vendor_pull_date) from read_parquet('{DATASET.table_file}')")
    return d


def thin(today: dt.date, *, keep_days: int = KEEP_DAYS, dry_run: bool = False) -> list[Path]:
    """Delete the vendor pulls older than keep_days, except the first pull and the
    newest pull of each ISO week. Return the deleted directories."""
    pulls = vendor_pulls()
    if not pulls:
        return []
    keep = {pulls[0][0], pulls[-1][0]}
    week_newest: dict[tuple[int, int], dt.date] = {}
    for d, _ in pulls:
        if (today - d).days <= keep_days:
            keep.add(d)
        else:
            iso = d.isocalendar()
            week_newest[(iso.year, iso.week)] = d
    keep.update(week_newest.values())
    doomed = [p for d, p in pulls if d not in keep]
    for p in doomed:
        if dry_run:
            log.info("%s: dry run. The pull to delete is %s.", DATASET.name, p)
        else:
            shutil.rmtree(p)
            log.info("%s: deleted %s.", DATASET.name, p)
    return doomed


def ingest(pull_date: dt.date | None = None, *, force: bool = False,
           rebuild: bool = False) -> Path:
    """Pull every series and publish the fold of the kept pulls. Without force, do
    nothing when the published table has a pull of pull_date or later. rebuild folds
    the vendor pulls and does not download."""
    pull_date = pull_date or dt.datetime.now(dt.UTC).date()
    last = published_pull()
    if not (force or rebuild) and last and (pull_date - last).days < MAX_AGE_DAYS:
        log.info("%s is from the pull of %s.", DATASET.name, last)
        return DATASET.table_file
    if not rebuild:
        fetch(pull_date)
    pulls = vendor_pulls()
    if not pulls:
        raise FileNotFoundError(f"{DATASET.name} has no vendor pull to build from.")
    staged = build(pulls)
    try:
        n = audit(staged, pulls[-1][0])
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    path = publish(staged, DATASET.table_file)
    log.info("Published %s: %s rows from %s pulls.", path, n, len(pulls))
    if not rebuild:
        thin(pull_date)
    return path


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.fred",
                                description="Pull the FRED series.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true", help="Pull now, whatever the age.")
    mode.add_argument("--rebuild", action="store_true",
                      help="Fold the vendor pulls again. Do not download.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(ingest(force=args.force, rebuild=args.rebuild))


if __name__ == "__main__":
    main()

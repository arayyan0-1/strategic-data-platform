"""Monthly returns of regions, countries and long US portfolios from the French data library.

    python -m sdp.ingest.french_international             # pull when the table is a week old
    python -m sdp.ingest.french_international --force     # pull now
    python -m sdp.ingest.french_international --rebuild   # build again from the newest vendor pull

The table is current state: each file states the whole history and the library revises it.
It is long: one row per file, series, column and month, with the value as a fraction. A series
is the table inside a file: `monthly` or `value_weighted` for a single-table file, the country
for the country file. The first monthly section of each file is read (value-weighted, and
for countries the dollar returns of all stocks). A missing value (-99.99) has no row.
vendor/ keeps the zip files of each pull.
"""
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import io
import logging
import os
import zipfile
from pathlib import Path

import duckdb

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import AuditFailure, get_public, one, publish, rows_to_parquet
from sdp.ingest.rest import _temp_beside

log = logging.getLogger(__name__)

DATASET = dal.FRENCH_INTERNATIONAL

REGIONS = ("Developed", "Developed_ex_US", "North_America", "Europe", "Japan",
           "Asia_Pacific_ex_Japan", "Emerging")
MOMENTUM = ("Developed_Mom", "Developed_ex_US_Mom", "North_America_Mom", "Europe_Mom",
            "Japan_Mom", "Asia_Pacific_ex_Japan_MOM", "Emerging_MOM")
COUNTRIES = "F-F_International_Countries.zip"

MONTHLY = "monthly"
VALUE_WEIGHTED = "value_weighted"

# File -> series. The country file has a series per country, named by its members.
FILES: dict[str, str | None] = {
    **{f"{r}_5_Factors_CSV.zip": MONTHLY for r in REGIONS},
    **{f"{m}_Factor_CSV.zip": MONTHLY for m in MOMENTUM},
    COUNTRIES: None,
    "F-F_Research_Data_Factors_CSV.zip": MONTHLY,
    "6_Portfolios_2x3_CSV.zip": VALUE_WEIGHTED,
    "6_Portfolios_ME_Prior_12_2_CSV.zip": VALUE_WEIGHTED,
    "6_Portfolios_ME_OP_2x3_CSV.zip": VALUE_WEIGHTED,
    "25_Portfolios_ME_VAR_5x5_CSV.zip": VALUE_WEIGHTED,
}

# The member files of the country archive. Their names are cut to eight letters.
COUNTRY_NAMES = {
    "uk": "United Kingdom", "austria": "Austria", "austrlia": "Australia", "belgium": "Belgium",
    "canada": "Canada", "denmark": "Denmark", "finland": "Finland", "france": "France",
    "germany": "Germany", "hongkong": "Hong Kong", "ireland": "Ireland", "italy": "Italy",
    "japan": "Japan", "malaysia": "Malaysia", "nethrlnd": "Netherlands",
    "newzland": "New Zealand", "norway": "Norway", "singapor": "Singapore", "spain": "Spain",
    "sweden": "Sweden", "swtzrlnd": "Switzerland",
}
COUNTRY_COLUMN = "Mkt"
COUNTRY_VALUES = 10   # The market, then four pairs of high and low and a zero-yield column.

COLUMNS = {"file": "VARCHAR", "series": "VARCHAR", "column_name": "VARCHAR", "month": "DATE",
           "value": "DOUBLE", "vendor_pull_date": "DATE"}
KEY = "file, series, column_name, month"

MISSING = (-99.99, -999.0)
MAX_AGE_DAYS = 7
MAX_ABS = 2.0         # Sanity bound on a monthly return (200 percent).
STALE_MONTHS = 4      # The library lags about two months; warn beyond this.

Rows = list[tuple[str, str, float]]
"""(column, month as YYYYMM, value as a fraction) of one table."""


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
    return get_public(settings.french_base_url + name, attempts)


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


def _month(code: str) -> str:
    """Return the last day of a YYYYMM month as an ISO date."""
    year, month = int(code[:4]), int(code[4:])
    if not 1 <= month <= 12:
        raise AuditFailure(f"The file has the month {code}.")
    return f"{year:04d}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"


def _value(text: str) -> float | None:
    x = float(text)
    return None if x in MISSING else round(x / 100, 8)


def _zip_text(data: bytes, index: int = 0) -> str:
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        return z.read(z.namelist()[index]).decode("latin-1")


def _is_row(first: str) -> bool:
    return len(first) == 6 and first.isdigit()


def parse_csv(data: bytes) -> Rows:
    """Return the first monthly section of a comma-separated zip file.

    The section starts at the first line that begins with a comma (the column names) and
    ends at the first text line after its rows. An annual row has a four-digit date and is
    never reached. A row with the wrong count of values raises, because stopping there would
    drop the rest of the history without a sign.
    """
    lines = _zip_text(data).splitlines()
    start = next((i for i, line in enumerate(lines) if line.startswith(",")), None)
    if start is None:
        raise AuditFailure("The file has no line of column names.")
    cols = [c.strip() for c in lines[start].split(",")[1:]]
    rows: Rows = []
    for line in lines[start + 1:]:
        parts = [p.strip() for p in line.split(",")]
        if _is_row(parts[0]):
            if len(parts) != len(cols) + 1:
                raise AuditFailure(f"The row {line.strip()!r} has {len(parts) - 1} values "
                                   f"and the file has {len(cols)} columns.")
            for col, text in zip(cols, parts[1:], strict=True):
                if (v := _value(text)) is not None:
                    rows.append((col, parts[0], v))
        elif rows and line.strip():
            break
    return rows


def parse_countries(data: bytes) -> dict[str, Rows]:
    """Return the market column of the first section of each country, by country name.

    A member is a space-separated table of dollar returns with the market first. A member
    that is not in COUNTRY_NAMES is skipped with a warning.
    """
    out: dict[str, Rows] = {}
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for info in z.infolist():
            stem = Path(info.filename).stem.lower()
            if stem not in COUNTRY_NAMES:
                log.warning("The country file has the unknown member %s.", info.filename)
                continue
            lines = z.read(info).decode("latin-1").splitlines()
            first = next((i for i, line in enumerate(lines)
                          if line.split() and _is_row(line.split()[0])), None)
            if first is None or first < 3:
                raise AuditFailure(f"{info.filename} has no monthly rows.")
            title = " ".join(lines[first - 3].split())
            if "Dollar" not in title or "Not Reqd" not in title \
                    or lines[first - 1].split()[:1] != [COUNTRY_COLUMN]:
                raise AuditFailure(f"The first section of {info.filename} is {title!r}. "
                                   f"The parser expects dollar returns of all stocks.")
            rows: Rows = []
            for line in lines[first:]:
                parts = line.split()
                if parts and _is_row(parts[0]):
                    if len(parts) != COUNTRY_VALUES + 1:
                        raise AuditFailure(f"The row {line.strip()!r} of {info.filename} has "
                                           f"{len(parts) - 1} values.")
                    if (v := _value(parts[1])) is not None:
                        rows.append((COUNTRY_COLUMN, parts[0], v))
                elif rows and line.strip():
                    break
            out[COUNTRY_NAMES[stem]] = rows
    return out


def build(pull: Path, pull_date: dt.date) -> Path:
    """Build the table from one vendor pull into _staging/. Return the staged file."""
    table: list[tuple] = []
    for name, series in FILES.items():
        data = (pull / name).read_bytes()
        by_series = parse_countries(data) if series is None else {series: parse_csv(data)}
        for s, rows in by_series.items():
            table += [(name, s, col, _month(code), v, pull_date) for col, code, v in rows]
    table.sort()
    staged = settings.staging_dir / f"{DATASET.name}-{os.getpid()}.parquet"
    rows_to_parquet(table, COLUMNS, staged)
    return staged


def _months_between(earlier: dt.date, later: dt.date) -> int:
    return (later.year - earlier.year) * 12 + later.month - earlier.month


def audit(staged: Path, pull_date: dt.date) -> int:
    """Raise AuditFailure on a table that would give wrong numbers; return the row count."""
    n, dups, nulls, worst = one(f"""
        select count(*), count(*) - count(distinct ({KEY})),
               count(*) filter (where file is null or series is null or column_name is null
                                or month is null or value is null or isnan(value)),
               max(abs(value))
        from read_parquet('{staged}')""")
    fatal = []
    if dups:
        fatal.append(f"{dups} duplicate (file, series, column, month) rows")
    if nulls:
        fatal.append(f"{nulls} rows with a null key or value")
    if worst is not None and worst >= MAX_ABS:
        fatal.append(f"a monthly return of {worst}, against a limit of {MAX_ABS}")
    last = dict(duckdb.execute(
        f"select file, max(month) from read_parquet('{staged}') group by 1").fetchall())
    empty = [name for name in FILES if name not in last]
    if empty:
        fatal.append("no rows in " + ", ".join(empty))
    if DATASET.table_file.exists():
        (lost,) = one(f"""
            select count(*) from read_parquet('{DATASET.table_file}') p
            anti join read_parquet('{staged}') s
              on p.file = s.file and p.series = s.series and p.column_name = s.column_name
                 and p.month = s.month""")
        if lost:
            fatal.append(f"{lost} rows of the published table are not in the pull. "
                         f"The library does not remove history")
    if fatal:
        raise AuditFailure(f"{DATASET.name}: " + ". ".join(fatal) + ".")
    for name, month in sorted(last.items()):
        if _months_between(month, pull_date) > STALE_MONTHS:
            log.warning("%s ends in %s, more than %s months before the pull.", name,
                        month, STALE_MONTHS)
    return n


def published_pull() -> dt.date | None:
    """Return the vendor pull date of the published table, or None."""
    if not DATASET.table_file.exists():
        return None
    (d,) = one(f"select max(vendor_pull_date) from read_parquet('{DATASET.table_file}')")
    return d


def ingest(pull_date: dt.date | None = None, *, force: bool = False,
           rebuild: bool = False) -> Path:
    """Pull the library files and publish the table.

    Without force, do nothing while the published table is younger than MAX_AGE_DAYS.
    rebuild reads the newest vendor pull without downloading.
    """
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
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.french_international",
                                description="Pull the French library's international and "
                                            "long US monthly returns.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true", help="Pull now, whatever the age.")
    mode.add_argument("--rebuild", action="store_true",
                      help="Build again from the newest vendor pull. Do not download.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(ingest(force=args.force, rebuild=args.rebuild))


if __name__ == "__main__":
    main()

"""Ingest SSGA's daily NAV, shares outstanding and net assets per ETF since inception.

    python -m sdp.ingest.etf_nav_history            # pull when the table is a month old
    python -m sdp.ingest.etf_nav_history --force    # pull now
    python -m sdp.ingest.etf_nav_history --rebuild  # fold the vendor pulls again

Each fund's file restates its whole history. The table folds every kept pull, newest
pull first, so a pull fills the days the daily screener missed while the machine was off.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import shutil
from pathlib import Path

import openpyxl

from sdp import dal
from sdp.config import settings
from sdp.ingest.common import (
    AuditFailure,
    get_public,
    one,
    publish,
    rows_to_parquet,
)
from sdp.ingest.rest import _temp_beside

log = logging.getLogger(__name__)

DATASET = dal.ETF_NAV_HISTORY
MAX_AGE_DAYS = 30
KEEP_PULLS = 2  # Besides the first pull, keep this many of the newest.
MIN_FUNDS = 100

COLUMNS = {"fund": "varchar", "date": "date", "nav": "double",
           "shares_outstanding": "double", "total_net_assets": "double",
           "vendor_pull_date": "date"}


def pull_dir(d: dt.date) -> Path:
    return settings.vendor_dir / DATASET.name / f"{d:%Y-%m-%d}"


def vendor_pulls() -> list[tuple[dt.date, Path]]:
    root = settings.vendor_dir / DATASET.name
    out = []
    for p in sorted(root.glob("????-??-??")) if root.exists() else []:
        try:
            out.append((dt.date.fromisoformat(p.name), p))
        except ValueError:
            continue
    return out


def ssga_funds() -> list[str]:
    """Return SSGA's ETF tickers from the newest screener snapshot, or from SSGA directly."""
    parts = dal.partitions(dal.ETF_FUNDS)
    if parts:
        rows = dal.on_date(dal.ETF_FUNDS, parts[-1]).filter("issuer = 'SSGA'") \
            .project("fund").fetchall()
        return sorted(r[0] for r in rows)
    from sdp.ingest.etf_funds import parse_ssga
    doc = json.loads(get_public(settings.ssga_fundfinder_url))
    return sorted(r[2] for r in parse_ssga(doc, dt.date.today()) if r[2])


def fetch(d: dt.date, funds: list[str]) -> Path:
    """Download each fund's NAV history into the vendor directory of d.

    A fund with no history file (new, or not an ETF SSGA reports on) is logged and skipped.
    """
    out = pull_dir(d)
    out.mkdir(parents=True, exist_ok=True)
    got = 0
    for fund in funds:
        url = f"{settings.ssga_fund_data_url}/navhist-us-en-{fund.lower()}.xlsx"
        try:
            data = get_public(url, attempts=3)
        except RuntimeError as exc:
            log.info("%s: no NAV history (%s).", fund, exc)
            continue
        if not data.startswith(b"PK"):
            log.info("%s: the NAV history is not a spreadsheet.", fund)
            continue
        tmp = _temp_beside(out / f"{fund}.xlsx")
        tmp.write_bytes(data)
        tmp.replace(out / f"{fund}.xlsx")
        got += 1
    log.info("Wrote %s of %s NAV histories to %s", got, len(funds), out)
    return out


def _number(v) -> float | None:
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", "").replace("$", ""))
    except ValueError:
        return None


def _date(v) -> dt.date | None:
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    for fmt in ("%d-%b-%Y", "%m/%d/%Y", "%Y-%m-%d"):
        try:
            return dt.datetime.strptime(str(v).strip(), fmt).date()
        except ValueError:
            continue
    return None


def parse(path: Path) -> list[tuple]:
    """Return (date, nav, shares outstanding, net assets) rows of one fund's file."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        rows = wb.worksheets[0].iter_rows(values_only=True)
        idx: dict[str, int] | None = None
        out = []
        for r in rows:
            cells = [c for c in r]
            if idx is None:
                if cells and str(cells[0]).strip() == "Date":
                    idx = {str(c).strip(): i for i, c in enumerate(cells) if c is not None}
                continue
            d = _date(cells[0]) if cells else None
            if d is None:
                if out:
                    break  # The table ended; the rest is disclaimer.
                continue
            row = (d, _number(cells[idx["NAV"]]) if "NAV" in idx else None,
                   _number(cells[idx["Shares Outstanding"]])
                   if "Shares Outstanding" in idx else None,
                   _number(cells[idx["Total Net Assets"]])
                   if "Total Net Assets" in idx else None)
            if not any(row[1:]):
                continue  # SSGA writes zeros, not blanks, for a day with no value (launch days).
            out.append(row)
        return out
    finally:
        wb.close()


def build(pulls: list[tuple[dt.date, Path]]) -> Path:
    """Fold the vendor pulls into one table: each (fund, date) from its newest pull."""
    newest: dict[tuple[str, dt.date], tuple] = {}
    for pull_date, p in pulls:  # Oldest first, so a newer pull overwrites.
        for f in sorted(p.glob("*.xlsx")):
            for d, nav, so, tna in parse(f):
                newest[(f.stem, d)] = (f.stem, d, nav, so, tna, pull_date)
    staged = settings.staging_dir / f"{DATASET.name}.parquet"
    rows_to_parquet(sorted(newest.values()), COLUMNS, staged)
    return staged


def audit(staged: Path) -> int:
    n, funds, dups, bad = one(f"""
        select count(*), count(distinct fund), count(*) - count(distinct (fund, date)),
               count(*) filter (where nav <= 0 or shares_outstanding < 0)
        from read_parquet('{staged}')""")
    fatal = []
    if funds < MIN_FUNDS:
        fatal.append(f"{funds} funds. The minimum is {MIN_FUNDS}")
    if dups:
        fatal.append(f"{dups} duplicate (fund, date) rows")
    if bad:
        fatal.append(f"{bad} rows with a NAV at or below zero or negative shares")
    if DATASET.table_file.exists():
        (before,) = one(f"select count(*) from read_parquet('{DATASET.table_file}')")
        if n < before:
            fatal.append(f"{n} rows against {before} published. A fold does not lose a row")
    if fatal:
        raise AuditFailure(f"{DATASET.name}: " + ". ".join(fatal) + ".")
    return n


def published_pull() -> dt.date | None:
    if not DATASET.table_file.exists():
        return None
    (d,) = one(f"select max(vendor_pull_date) from read_parquet('{DATASET.table_file}')")
    return d


def thin() -> list[Path]:
    """Delete vendor pulls except the first and the newest KEEP_PULLS."""
    pulls = vendor_pulls()
    keep = {pulls[0][0], *(d for d, _ in pulls[-KEEP_PULLS:])} if pulls else set()
    doomed = [p for d, p in pulls if d not in keep]
    for p in doomed:
        shutil.rmtree(p)
        log.info("%s: deleted %s.", DATASET.name, p)
    return doomed


def ingest(pull_date: dt.date | None = None, *, force: bool = False,
           rebuild: bool = False) -> Path:
    """Pull every SSGA fund's history when the table is MAX_AGE_DAYS old, and publish."""
    pull_date = pull_date or dt.datetime.now(dt.UTC).date()
    last = published_pull()
    if not (force or rebuild) and last and (pull_date - last).days < MAX_AGE_DAYS:
        log.info("%s is from the pull of %s.", DATASET.name, last)
        return DATASET.table_file
    if not rebuild:
        fetch(pull_date, ssga_funds())
    pulls = vendor_pulls()
    if not pulls:
        raise FileNotFoundError(f"{DATASET.name} has no vendor pull to build from.")
    staged = build(pulls)
    try:
        n = audit(staged)
    except BaseException:
        staged.unlink(missing_ok=True)
        raise
    path = publish(staged, DATASET.table_file)
    log.info("Published %s: %s rows from %s pulls.", path, n, len(pulls))
    if not rebuild:
        thin()
    return path


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m sdp.ingest.etf_nav_history",
                                description="Pull SSGA's ETF NAV histories.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true", help="Pull now, whatever the age.")
    mode.add_argument("--rebuild", action="store_true",
                      help="Fold the vendor pulls again. Do not download.")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    print(ingest(force=args.force, rebuild=args.rebuild))


if __name__ == "__main__":
    main()

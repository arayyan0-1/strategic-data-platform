# src/sdp/ingest/massive_corporate_actions.py
"""Splits and dividends. One table for each, replaced by every pull.

The endpoints give current state, so raw/ holds one table per dataset with no
date in the path. vendor/ keeps the pulls by date, and sdp.restatement reads
them. For dividends, vendor/ keeps one pull per ISO week after 30 days. The flow
is Write-Audit-Publish: fetch into vendor/, build() into _staging/, audit(),
os.replace into raw/. The replace is atomic, and a failed audit leaves the
previous pull in place.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import re
from pathlib import Path

import duckdb

from sdp import dal
from sdp.config import settings
from sdp.ingest.rest import dump_ndjson

log = logging.getLogger(__name__)

SPECS = {
    "massive_splits": {
        "path": "/stocks/v1/splits",
        "params": {"limit": 5000, "sort": "execution_date.asc"},
    },
    "massive_dividends": {
        "path": "/stocks/v1/dividends",
        "params": {"limit": 5000, "sort": "ex_dividend_date.asc"},
    },
}

# A dividend pull is about 42 MB, so ingest() deletes old pulls of these datasets.
THINNED = ("massive_dividends",)


def raw_path(dataset: str) -> Path:
    """Return the single published table of a current-state dataset."""
    return settings.raw_dir / dataset / "data.parquet"


def _one(sql: str, con: duckdb.DuckDBPyConnection | None = None) -> tuple:
    row = (con or duckdb).execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"The query returned no rows: {sql[:120]}")
    return row


def _vendor_files(dataset: str, pull_date: dt.date) -> list[Path]:
    """Return the vendor files of one pull date that exist, gzip or plain."""
    vdir = settings.vendor_dir / dataset
    names = (f"{pull_date:%Y-%m-%d}.ndjson.gz", f"{pull_date:%Y-%m-%d}.ndjson")
    return [vdir / n for n in names if (vdir / n).exists()]


def _today() -> dt.date:
    return dt.datetime.now(dt.UTC).date()


def _published_pull(dataset: str) -> dt.date | None:
    """Return the vendor_pull_date of the published table, or None if no table exists."""
    ds = dal.DATASETS[dataset]
    if not ds.table_file.exists():
        return None
    row = dal.current(ds).aggregate("max(vendor_pull_date)").fetchone()
    return row[0] if row else None


def _unchanged(dataset: str, pull_date: dt.date) -> bool:
    """Return True if the vendor file of pull_date built the published table and
    did not change after the publish."""
    files = _vendor_files(dataset, pull_date)
    if not files:
        return False
    try:
        if _published_pull(dataset) != pull_date:
            return False
    except duckdb.Error:
        # The query cannot read the table, so a new publish replaces it.
        return False
    newest_write = max(f.stat().st_mtime_ns for f in files)
    return raw_path(dataset).stat().st_mtime_ns >= newest_write


class AuditFailure(RuntimeError):
    pass


# ---------- BUILD ----------

def build(dataset: str, vendor_file: Path, pull_date: dt.date) -> Path:
    """Stage the whole table from one vendor pull. vendor_pull_date is
    provenance, not a key: it names the vendor file that built the table."""
    staged = settings.staging_dir / dataset / f"{pull_date:%Y-%m-%d}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)

    duckdb.execute(f"""
        copy (
            select
                *,
                date '{pull_date:%Y-%m-%d}' as vendor_pull_date
            from read_json(
                '{vendor_file}',
                format = 'newline_delimited',
                sample_size = -1
            )
        ) to '{staged}' (format parquet, compression zstd)
    """)
    return staged

# ---------- AUDIT ----------

MAX_NEW_BAD_TICKERS = 3   # Tickers that become defective in one pull.
RATIO_HI, RATIO_LO = 10000.0, 0.0001


def _audit_common(staged: Path, dataset: str) -> int:
    n_rows, n_null_id, n_null_ticker = _one(f"""
        select count(*),
               count(*) filter (id is null),
               count(*) filter (ticker is null)
        from read_parquet('{staged}')
    """)

    problems = []
    if n_rows == 0:
        problems.append("zero rows")
    if n_null_id:
        problems.append(f"{n_null_id} null ids")
    if n_null_ticker:
        problems.append(f"{n_null_ticker} null tickers")

    dupes = _one(
        f"select count(*) from (select id from read_parquet('{staged}') "
        f"group by id having count(*) > 1)"
    )[0]
    if dupes:
        problems.append(f"{dupes} duplicate ids")

    if problems:
        raise AuditFailure(f"{dataset}: " + ". ".join(problems) + ".")
    return n_rows


def _audit_splits(staged: Path, pull_date: dt.date) -> None:
    """The past and future divide at the pull date, not today. A null factor is
    fatal only on an event dated before the pull date. The vendor fills the factor
    of an event on the pull date later that day, so that event is still pending.
    Comparing against current_date would fail a later rebuild of a pull that
    passed when live."""
    (null_past, null_future, zero_f, neg_f, null_date,
     bad_fwd, bad_rev, bad_stk, bad_from, bad_to, extreme) = _one(f"""
        select
            count(*) filter (historical_adjustment_factor is null
                             and execution_date <  date '{pull_date:%Y-%m-%d}'),
            count(*) filter (historical_adjustment_factor is null
                             and execution_date >= date '{pull_date:%Y-%m-%d}'),
            count(*) filter (historical_adjustment_factor = 0),
            count(*) filter (historical_adjustment_factor < 0),
            count(*) filter (execution_date is null),
            count(*) filter (adjustment_type = 'forward_split'
                             and split_to <= split_from),
            count(*) filter (adjustment_type = 'reverse_split'
                             and split_to >= split_from),
            count(*) filter (adjustment_type = 'stock_dividend'
                             and split_to <= split_from),
            count(*) filter (split_from is null or split_from <= 0),
            count(*) filter (split_to   is null or split_to   <= 0),
            count(*) filter (split_to / split_from > {RATIO_HI}
                             or split_to / split_from < {RATIO_LO})
        from read_parquet('{staged}')
    """)

    fatal = []
    if null_date:
        fatal.append(f"{null_date} null execution_date")
    if null_past:
        fatal.append(f"{null_past} null factors on already-executed events")
    for label, n in [("forward_split", bad_fwd), ("reverse_split", bad_rev),
                     ("stock_dividend", bad_stk)]:
        if n:
            fatal.append(f"{n} {label} rows have a ratio in the wrong direction")
    if bad_from or bad_to:
        fatal.append(f"{bad_from + bad_to} rows have an invalid split ratio")

    if fatal:
        raise AuditFailure("splits: " + ". ".join(fatal) + ".")

    unusable = zero_f + neg_f
    if unusable:
        log.warning("splits: %s factors are not positive. Staging removes these rows.",
                    unusable)
    if null_future:
        log.info("splits: %s events are pending. The vendor gives no factor for them.",
                 null_future)
    if extreme:
        log.warning("splits: %s events have a ratio outside the limits [%s, %s].",
                    extreme, RATIO_LO, RATIO_HI)


def _audit_dividends(staged: Path, pull_date: dt.date) -> None:
    (null_ex, bad_cash,
     null_past, null_past_tickers, null_future,
     bad_factor, bad_factor_tickers,
     non_usd, bad_freq) = _one(f"""
        select
            -- Fatal. These rows are unusable.
            count(*) filter (ex_dividend_date is null),
            count(*) filter (cash_amount is null or cash_amount < 0),

            -- Expected. The vendor has no price to compute a factor with.
            count(*) filter (historical_adjustment_factor is null
                             and ex_dividend_date <= date '{pull_date:%Y-%m-%d}'),
            count(distinct ticker) filter (historical_adjustment_factor is null
                                           and ex_dividend_date <= date '{pull_date:%Y-%m-%d}'),
            count(*) filter (historical_adjustment_factor is null
                             and ex_dividend_date > date '{pull_date:%Y-%m-%d}'),

            -- Known defects. The delta check judges these, not the count.
            count(*) filter (historical_adjustment_factor <= 0),
            count(distinct ticker) filter (historical_adjustment_factor <= 0),

            -- Schema tripwires.
            count(*) filter (currency is not null and currency <> 'USD'),
            count(*) filter (frequency is null or frequency not in (0,1,2,3,4,12,24,52,104,365))
        from read_parquet('{staged}')
    """)

    fatal = []
    if null_ex:
        fatal.append(f"{null_ex} rows have a null ex_dividend_date")
    if bad_cash:
        fatal.append(f"{bad_cash} rows have a null or negative cash_amount")
    if fatal:
        raise AuditFailure("dividends: " + ". ".join(fatal) + ".")

    # A null dividend factor is structural, not fatal: computing it needs a
    # price on the ex-date, and the vendor has none for that security.
    if null_past:
        log.info("dividends: %s null factors on past ex-dates, on %s tickers. "
                 "The vendor has no price to compute them with.",
                 null_past, null_past_tickers)
    if null_future:
        log.info("dividends: %s are announced and not yet ex.", null_future)
    if bad_factor:
        log.warning("dividends: %s factors are not positive, on %s tickers. "
                    "The delta check finds the new ones.",
                    bad_factor, bad_factor_tickers)
    if non_usd:
        log.info("dividends: %s rows are not in USD. Staging removes them.", non_usd)
    if bad_freq:
        log.warning("dividends: %s rows have an unexpected frequency value.", bad_freq)


def _audit_vs_previous(dataset: str, staged: Path, n_rows: int) -> None:
    """Compare the staged table against the published one."""
    prev = raw_path(dataset)
    if not prev.exists():
        log.info("%s: first pull. %s rows. There is no baseline.", dataset, n_rows)
        return

    prev_n = _one(f"select count(*) from read_parquet('{prev}')")[0]
    if n_rows < prev_n * 0.99:
        raise AuditFailure(
            f"{dataset}: {n_rows} rows against {prev_n} in the published table. "
            f"The history became smaller."
        )

    new_bad = duckdb.execute(f"""
        select ticker, count(*) as n
        from read_parquet('{staged}')
        where historical_adjustment_factor <= 0
          and ticker not in (
              select ticker from read_parquet('{prev}')
              where historical_adjustment_factor <= 0
          )
        group by ticker order by n desc
    """).fetchall()

    if len(new_bad) > MAX_NEW_BAD_TICKERS:
        raise AuditFailure(
            f"{dataset}: {len(new_bad)} tickers became defective in this pull: "
            f"{new_bad[:10]}"
        )
    if new_bad:
        log.warning("%s: these tickers have new factors that are not positive: %s",
                    dataset, new_bad)

    log.info("%s: %s rows. Change of %s against the published table.",
             dataset, n_rows, n_rows - prev_n)

# ---------- PUBLISH ----------

def _publish(dataset: str, vendor_file: Path, pull_date: dt.date) -> Path:
    """Build the table, audit it, and replace the published table."""
    staged = build(dataset, vendor_file, pull_date)
    try:
        n_rows = _audit_common(staged, dataset)
        (_audit_splits if dataset == "massive_splits" else _audit_dividends)(
            staged, pull_date)
        _audit_vs_previous(dataset, staged, n_rows)

        dest = raw_path(dataset)
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, dest)
        log.info("Published %s: %s rows from the pull of %s.",
                 dest, n_rows, pull_date)
        return dest
    finally:
        staged.unlink(missing_ok=True)


def ingest(dataset: str, pull_date: dt.date | None = None, *,
           force: bool = False) -> Path | None:
    """Fetch the pull of the day and replace the published table. If the vendor
    file of that date built the published table and did not change after it, the
    table and its mtime do not change. Force fetches the pull again and publishes it."""
    pull_date = pull_date or _today()
    spec = SPECS[dataset]
    if not force and _unchanged(dataset, pull_date):
        log.info("%s: the published table is from the pull of %s. The table does "
                 "not change.", dataset, pull_date)
        return raw_path(dataset)

    vendor_file = dump_ndjson(dataset, spec["path"], spec["params"], pull_date,
                              force=force, compress=True)
    dest = _publish(dataset, vendor_file, pull_date)
    if dataset in THINNED:
        # The table is published. A failure here must not fail the pull.
        try:
            thin_vendor_pulls(dataset)
        except Exception as exc:
            log.error("%s: thin_vendor_pulls failed. The table is published. %s: %s",
                      dataset, type(exc).__name__, exc)
    return dest

# ---------- REBUILD ----------

_VENDOR_NAME = re.compile(r"^(\d{4}-\d{2}-\d{2})\.ndjson(\.gz)?$")


def vendor_pulls(dataset: str) -> list[tuple[dt.date, Path]]:
    """Return every vendor pull file with its date, in pull order."""
    vdir = settings.vendor_dir / dataset
    if not vdir.exists():
        return []
    found = {}
    for p in sorted(vdir.iterdir()):
        m = _VENDOR_NAME.match(p.name)
        if m:
            found[dt.date.fromisoformat(m.group(1))] = p
    return sorted(found.items())


def rebuild(dataset: str, pull_date: dt.date | None = None) -> Path:
    """Build the table again from a vendor pull, with no fetch. The default is
    the newest pull. Name an older pull to rebuild the table as that pull
    stated it."""
    pulls = vendor_pulls(dataset)
    if not pulls:
        raise FileNotFoundError(
            f"{dataset}: no vendor files in {settings.vendor_dir / dataset}. "
            f"A rebuild reads vendor/ and cannot run without it."
        )
    available = dict(pulls)
    if pull_date is None:
        pull_date = pulls[-1][0]
    elif pull_date not in available:
        raise FileNotFoundError(
            f"{dataset}: no vendor pull for {pull_date}. Available: "
            f"{sorted(available)}."
        )
    return _publish(dataset, available[pull_date], pull_date)

# ---------- THIN ----------

def pinned_pulls(dataset: str) -> set[dt.date]:
    """Return the pull dates that thinning must keep, one ISO date per line of
    pinned.txt in the vendor directory. A line that starts with # is a comment."""
    path = settings.vendor_dir / dataset / "pinned.txt"
    if not path.exists():
        return set()
    pins = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.split("#", 1)[0].strip()
        if text:
            pins.add(dt.date.fromisoformat(text))
    return pins


def thin_vendor_pulls(dataset: str, *, keep_days: int = 30,
                      dry_run: bool = False) -> list[Path]:
    """Delete old vendor pulls and keep one pull per ISO week. Keep the first pull,
    each pull in the last keep_days days, the pull of the published table, and each
    date in pinned.txt beside the pulls.

    Return the deleted files. With dry_run, return the files to delete and delete
    nothing.
    """
    pulls = vendor_pulls(dataset)
    if not pulls:
        return []
    # A vendor file with a future date must not move the window.
    newest = min(pulls[-1][0], _today())
    keep = {pulls[0][0], _published_pull(dataset), *pinned_pulls(dataset)}
    week_newest: dict[tuple[int, int], dt.date] = {}
    for d, _ in pulls:
        if (newest - d).days <= keep_days:
            keep.add(d)
        else:
            # The pulls are in date order, so the newest pull of a week wins.
            iso = d.isocalendar()
            week_newest[(iso.year, iso.week)] = d
    keep.update(week_newest.values())

    doomed = [p for d, _ in pulls if d not in keep for p in _vendor_files(dataset, d)]
    for path in doomed:
        if dry_run:
            log.info("%s: dry run. The file to delete is %s.", dataset, path)
        else:
            path.unlink(missing_ok=True)
            log.info("%s: deleted %s.", dataset, path)
    return doomed


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        prog="python -m sdp.ingest.massive_corporate_actions",
        description="Pull, rebuild or thin the corporate action datasets.",
    )
    p.add_argument("datasets", nargs="*",
                   help=f"Zero or more of: {', '.join(SPECS)}. The default is all.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true",
                      help="Fetch the pull again when the vendor file exists.")
    mode.add_argument("--rebuild", action="store_true",
                      help="Build the table from the newest vendor pull. Do not fetch.")
    mode.add_argument("--thin", action="store_true",
                      help="Delete old vendor pulls. Keep one per ISO week after 30 days. "
                           f"Use only with {', '.join(THINNED)}, the default.")
    p.add_argument("--dry-run", action="store_true",
                   help="With --thin, show the files to delete and delete nothing.")
    args = p.parse_intermixed_args(argv)
    unknown = [n for n in args.datasets if n not in SPECS]
    if unknown:
        p.error(f"The dataset {', '.join(unknown)} is unknown. "
                f"Use one of: {', '.join(SPECS)}.")
    if args.thin and any(n not in THINNED for n in args.datasets):
        p.error(f"Use --thin only with {', '.join(THINNED)}.")
    if args.dry_run and not args.thin:
        p.error("Use --dry-run only with --thin.")

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.thin:
        for name in args.datasets or list(THINNED):
            doomed = thin_vendor_pulls(name, dry_run=args.dry_run)
            for path in doomed:
                print(path)
            if args.dry_run:
                print(f"{name}: {len(doomed)} files to delete. Dry run. No file deleted.")
            else:
                print(f"{name}: {len(doomed)} files deleted.")
        return
    for name in args.datasets or list(SPECS):
        if args.rebuild:
            rebuild(name)
        else:
            ingest(name, force=args.force)


if __name__ == "__main__":
    main()

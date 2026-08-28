# src/sdp/ingest/massive_corporate_actions.py
"""Splits and dividends. One table for each, replaced by every pull.

The endpoints give current state, so raw/ holds one table per dataset with no
date in the path. vendor/ keeps every pull dated, which is what makes a past
belief recoverable and where sdp.restatement reads. The flow is
Write-Audit-Publish: fetch into vendor/, build() into _staging/, audit(),
os.replace into raw/. The replace is atomic, and a failed audit leaves the
previous pull in place.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
import re
from pathlib import Path

import duckdb

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


def raw_path(dataset: str) -> Path:
    """Return the single published table of a current-state dataset."""
    return settings.raw_dir / dataset / "data.parquet"


def _one(sql: str, con: duckdb.DuckDBPyConnection | None = None) -> tuple:
    row = (con or duckdb).execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"The query returned no rows: {sql[:120]}")
    return row


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
    fatal only on an event already executed at the pull. Comparing against
    current_date would fail a later rebuild of a pull that passed when live."""
    (null_past, null_future, zero_f, neg_f, null_date,
     bad_fwd, bad_rev, bad_stk, bad_from, bad_to, extreme) = _one(f"""
        select
            count(*) filter (historical_adjustment_factor is null
                             and execution_date <= date '{pull_date:%Y-%m-%d}'),
            count(*) filter (historical_adjustment_factor is null
                             and execution_date  > date '{pull_date:%Y-%m-%d}'),
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
    """Compare the staged table against the one published now. The sharp check
    is the delta between two pulls, not an absolute count, which on a growing
    dataset goes noisy. It stops a pull that loses history replacing a good one."""
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
    """Fetch the day's pull and replace the published table. An existing vendor
    file is not refetched unless force is set, but it is still built and
    published; there is no partition to skip."""
    pull_date = pull_date or dt.datetime.now(dt.UTC).date()
    spec = SPECS[dataset]
    vendor_file = dump_ndjson(dataset, spec["path"], spec["params"], pull_date,
                              force=force, compress=True)
    return _publish(dataset, vendor_file, pull_date)

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
    the newest pull. Name an older pull to rebuild the table as it stated it,
    the one path back to a past belief of the vendor."""
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


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    names = [t for t in sys.argv[1:] if not t.startswith("-")] or list(SPECS)
    for name in names:
        if "--rebuild" in sys.argv:
            rebuild(name)
        else:
            ingest(name, force="--force" in sys.argv)

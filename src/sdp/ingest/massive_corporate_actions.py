# src/sdp/ingest/massive_corporate_actions.py
from __future__ import annotations

import datetime as dt
import logging
import os
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


def raw_path(dataset: str, pull_date: dt.date) -> Path:
    return settings.raw_dir / dataset / f"pull_date={pull_date:%Y-%m-%d}" / "data.parquet"

def _one(sql: str) -> tuple:
    row = duckdb.execute(sql).fetchone()
    if row is None:
        raise RuntimeError(f"query returned no rows: {sql[:120]}")
    return row

# ---------- WRITE ----------

def build(dataset: str, vendor_file: Path, pull_date: dt.date) -> Path:
    staged = settings.staging_dir / dataset / f"{pull_date:%Y-%m-%d}.parquet"
    staged.parent.mkdir(parents=True, exist_ok=True)

    duckdb.execute(f"""
        copy (
            select
                *,
                date '{pull_date:%Y-%m-%d}' as pull_date
            from read_json(
                '{vendor_file}',
                format = 'newline_delimited',
                sample_size = -1
            )
        ) to '{staged}' (format parquet, compression zstd)
    """)
    return staged

# ---------- AUDIT ----------

MAX_NEW_BAD_TICKERS = 3   # newly defective tickers per pull
RATIO_HI, RATIO_LO = 10000.0, 0.0001
class AuditFailure(RuntimeError):
    pass

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
        raise AuditFailure(f"{dataset}: " + "; ".join(problems))
    return n_rows


def _audit_splits(staged: Path) -> None:
    (null_past, null_future, zero_f, neg_f, null_date,
     bad_fwd, bad_rev, bad_stk, bad_from, bad_to, extreme) = _one(f"""
        select
            count(*) filter (historical_adjustment_factor is null
                             and execution_date <= current_date),
            count(*) filter (historical_adjustment_factor is null
                             and execution_date  > current_date),
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
            fatal.append(f"{n} {label} rows with ratio in the wrong direction")
    if bad_from or bad_to:
        fatal.append(f"{bad_from + bad_to} rows with invalid split ratio components")

    if fatal:
        raise AuditFailure("splits: " + "; ".join(fatal))
    
    unusable = zero_f + neg_f
    if unusable:
        log.warning("splits: %s non-positive factors, excluded in staging", unusable)
    if null_future:
        log.info("splits: %s pending events, factor not yet assigned", null_future)
    if extreme:
        log.warning("splits: %s events outside ratio bounds [%s, %s]",
                    extreme, RATIO_LO, RATIO_HI)


def _audit_dividends(staged: Path) -> None:
    (null_ex, bad_cash,
     null_past, null_past_tickers, null_future,
     bad_factor, bad_factor_tickers,
     non_usd, bad_freq) = _one(f"""
        select
            -- fatal: structurally unusable
            count(*) filter (ex_dividend_date is null),
            count(*) filter (cash_amount is null or cash_amount < 0),

            -- expected: vendor has no price to compute a factor against
            count(*) filter (historical_adjustment_factor is null
                             and ex_dividend_date <= current_date),
            count(distinct ticker) filter (historical_adjustment_factor is null
                                           and ex_dividend_date <= current_date),
            count(*) filter (historical_adjustment_factor is null
                             and ex_dividend_date > current_date),

            -- known defects: judged by the delta check, not absolute count
            count(*) filter (historical_adjustment_factor <= 0),
            count(distinct ticker) filter (historical_adjustment_factor <= 0),

            -- schema tripwires
            count(*) filter (currency is not null and currency <> 'USD'),
            count(*) filter (frequency is null or frequency not in (0,1,2,3,4,12,24,52,104,365))
        from read_parquet('{staged}')
    """)

    fatal = []
    if null_ex:
        fatal.append(f"{null_ex} rows with null ex_dividend_date")
    if bad_cash:
        fatal.append(f"{bad_cash} rows with null or negative cash_amount")
    if fatal:
        raise AuditFailure("dividends: " + "; ".join(fatal))

    # Unlike splits, the dividend factor needs a price on the ex-date to compute
    # (product of 1 - D/P terms). Missing vendor price coverage -> null factor.
    # Structural, concentrated in the old and delisted tail. Not fatal.
    if null_past:
        log.info("dividends: %s null factors on past ex-dates across %s tickers "
                 "(no vendor price to compute against)", null_past, null_past_tickers)
    if null_future:
        log.info("dividends: %s announced but not yet ex", null_future)
    if bad_factor:
        log.warning("dividends: %s non-positive factors across %s tickers "
                    "(new ones caught by delta check)", bad_factor, bad_factor_tickers)
    if non_usd:
        log.info("dividends: %s non-USD rows, filtered in staging", non_usd)
    if bad_freq:
        log.warning("dividends: %s rows with unexpected frequency value", bad_freq)

        
def _audit_vs_previous(dataset: str, staged: Path, pull_date: dt.date, n_rows: int) -> None:
    root = settings.raw_dir / dataset
    current = f"pull_date={pull_date:%Y-%m-%d}"
    priors = sorted(
        p for p in root.glob("pull_date=*") if p.name != current
        ) if root.exists() else []
    if not priors:
        log.info("%s: first pull, %s rows, no baseline", dataset, n_rows)
        return
    prev = priors[-1] / "data.parquet"

    prev_n = _one(f"select count(*) from read_parquet('{prev}')")[0]
    if n_rows < prev_n * 0.99:
        raise AuditFailure(f"{dataset}: {n_rows} rows vs {prev_n} — history shrank")

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
        raise AuditFailure(f"{dataset}: {len(new_bad)} newly defective tickers: {new_bad[:10]}")
    if new_bad:
        log.warning("%s: new non-positive factors on %s", dataset, new_bad)

    log.info("%s: %s rows (+%s vs %s)", dataset, n_rows, n_rows - prev_n, priors[-1].name)

# ---------- PUBLISH ----------

def ingest(dataset: str, pull_date: dt.date | None = None, *, force: bool = False) -> Path | None:
    pull_date = pull_date or dt.datetime.now(dt.UTC).date()
    dest = raw_path(dataset, pull_date)
    if dest.exists() and not force:
        log.info("already pulled today: %s", dest)
        return dest

    spec = SPECS[dataset]
    vendor_file = dump_ndjson(dataset, spec["path"], spec["params"], pull_date, force=force)

    staged = build(dataset, vendor_file, pull_date)
    n_rows = _audit_common(staged, dataset)
    (_audit_splits if dataset == "massive_splits" else _audit_dividends)(staged)
    _audit_vs_previous(dataset, staged, pull_date, n_rows)

    dest.parent.mkdir(parents=True, exist_ok=True)
    os.replace(staged, dest)
    log.info("published %s", dest)
    return dest


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    targets = sys.argv[1:] or list(SPECS)
    for name in [t for t in targets if not t.startswith("-")]:
        ingest(name, force="--force" in sys.argv)
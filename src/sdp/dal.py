# src/sdp/dal.py
"""The only entry point for reads of the data lake.

Every read of published data uses this module. No other module opens a Parquet
file or a DuckDB database. This makes the point-in-time guarantees enforceable
and not aspirational. The rule for current-state datasets is written once, here.
No call site must remember it.

Every function returns a DuckDBPyRelation. Relations are lazy and you can chain
SQL onto them. Materialise only at the edge, in a notebook or a plot, with
.pl() or .fetchall(). Do not write pandas for anything you can write as a query.

This module does not adjust prices for corporate actions. Adjustment is a dbt
staging model. See docs/decisions/0005-adjustment-at-query-time.md. This module
supplies the raw facts and the action snapshots. The join between them is a
modelling decision and belongs in SQL that a reader can see.

Reads name the partition files. They do not glob 'date=*'. A date filter
therefore removes files before the scan and not after it. Hive partitioning is
off, because build() already writes 'date' and 'pull_date' as columns in the
file. The same name from two sources gives ambiguity and no benefit.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Literal

import duckdb

from sdp.config import settings

PartitionKey = Literal["date", "pull_date"]


@dataclass(frozen=True)
class Dataset:
    """A published dataset in raw/.

    The 'key' field records the central data-model distinction. See
    docs/decisions/0002-partition-key-by-dataset-kind.md.

    'date'
        Event stream. The partition holds the facts of one session. It is
        immutable. You can backfill it. Read a range of dates.
    'pull_date'
        Current state. The vendor gives its present belief about all of
        history. The endpoint has no as_of parameter. You cannot backfill it.
        Read the newest snapshot at or before the date that you simulate.
    """

    name: str
    key: PartitionKey
    union_by_name: bool = False
    """True for the datasets from REST. read_json infers the schema of each
    pull. If the vendor adds a field during a backfill, a read without this
    flag fails."""

    @property
    def root(self):
        return settings.raw_dir / self.name

    def partition_dir(self, d: dt.date):
        return self.root / f"{self.key}={d:%Y-%m-%d}"

    def partition_file(self, d: dt.date):
        return self.partition_dir(d) / "data.parquet"


DAY_AGGS = Dataset("us_stocks_day_aggs", "date")
TICKERS = Dataset("massive_tickers", "date", union_by_name=True)
SPLITS = Dataset("massive_splits", "pull_date", union_by_name=True)
DIVIDENDS = Dataset("massive_dividends", "pull_date", union_by_name=True)

DATASETS = {d.name: d for d in (DAY_AGGS, TICKERS, SPLITS, DIVIDENDS)}

_con: duckdb.DuckDBPyConnection | None = None


def con() -> duckdb.DuckDBPyConnection:
    """Return the shared in-memory connection that owns every relation.

    A relation belongs to the connection that made it. Code that joins two dal
    relations needs one connection for both. The connection is in memory,
    because raw/ holds Parquet files. The warehouse file belongs to dbt.
    """
    global _con
    if _con is None:
        _con = duckdb.connect()
        _con.execute("set TimeZone = 'UTC'")
    return _con


class MissingPartition(FileNotFoundError):
    """The requested partition is not published."""


# ---------- partition discovery ----------

def partitions(ds: Dataset) -> list[dt.date]:
    """Return every published partition date, in ascending order.

    The list is empty when no partition exists.
    """
    if not ds.root.exists():
        return []
    prefix = f"{ds.key}="
    found = []
    for p in ds.root.glob(f"{prefix}*"):
        try:
            d = dt.date.fromisoformat(p.name[len(prefix):])
        except ValueError:
            continue  # Not a partition directory. Ignore it.
        if ds.partition_file(d).exists():
            found.append(d)
    return sorted(found)


def coverage(ds: Dataset) -> tuple[dt.date, dt.date] | None:
    """Return the first and last published partition dates.

    Return None when no partition exists.
    """
    parts = partitions(ds)
    return (parts[0], parts[-1]) if parts else None


def _describe_coverage(ds: Dataset) -> str:
    parts = partitions(ds)
    if not parts:
        return f"{ds.name} has no published partitions"
    return f"{ds.name} has {len(parts)} partitions, from {parts[0]} to {parts[-1]}"


def _read(ds: Dataset, dates: list[dt.date]) -> duckdb.DuckDBPyRelation:
    files = [str(ds.partition_file(d)) for d in dates]
    return con().read_parquet(files, union_by_name=ds.union_by_name)


# ---------- event streams: read a range of dates ----------

def series(
    ds: Dataset,
    start: dt.date | None = None,
    end: dt.date | None = None,
) -> duckdb.DuckDBPyRelation:
    """Return the rows of every published partition from start to end.

    Both limits are inclusive. Each limit defaults to the limit of the
    published data. A missing session inside the range is not an error. A gap
    is normal during a backfill. Write-Audit-Publish leaves no partition when
    an audit fails. A gap therefore means "no data" and never "bad data". Use
    gaps() to find the missing sessions.
    """
    if ds.key != "date":
        raise ValueError(
            f"{ds.name} has the partition key pull_date. It holds current state "
            f"and not an event stream. A range read mixes vendor beliefs from "
            f"different days into one table. Use snapshot(ds, as_of) or history(ds)."
        )
    parts = partitions(ds)
    if not parts:
        raise MissingPartition(_describe_coverage(ds))
    lo = start or parts[0]
    hi = end or parts[-1]
    if lo > hi:
        raise ValueError(f"The start date {lo} is after the end date {hi}.")
    wanted = [d for d in parts if lo <= d <= hi]
    if not wanted:
        raise MissingPartition(
            f"{ds.name} has no partition between {lo} and {hi}. {_describe_coverage(ds)}."
        )
    return _read(ds, wanted)


def on_date(ds: Dataset, d: dt.date) -> duckdb.DuckDBPyRelation:
    """Return one session of an event stream.

    This function raises MissingPartition when the date has no partition.
    """
    if ds.key != "date":
        raise ValueError(
            f"{ds.name} has the partition key pull_date. Use snapshot(ds, as_of)."
        )
    if not ds.partition_file(d).exists():
        raise MissingPartition(
            f"{ds.name} has no partition for {d}. {_describe_coverage(ds)}."
        )
    return _read(ds, [d])


def gaps(ds: Dataset, start: dt.date, end: dt.date) -> list[dt.date]:
    """Return the XNYS sessions from start to end that have no partition.

    The import of exchange_calendars is inside the function. That import is
    slow and most reads do not need a calendar.
    """
    import exchange_calendars as xcals

    sessions = xcals.get_calendar("XNYS").sessions_in_range(
        start.isoformat(), end.isoformat()
    )
    have = set(partitions(ds))
    return [s.date() for s in sessions if s.date() not in have]


# ---------- current state: read one snapshot as of a date ----------

def snapshot(ds: Dataset, as_of: dt.date) -> duckdb.DuckDBPyRelation:
    """Return the newest pull at or before as_of. This is the point-in-time read.

    You must give as_of. There is no default value. A default value gives the
    latest pull, and that is the lookahead which this partition scheme
    prevents. For example, a corporate action snapshot taken today can decide a
    trade dated last year. The trade then uses every restatement that the
    vendor made between the two dates.

    This function raises MissingPartition when no pull is that old. That answer
    is correct and it is not a limit to avoid. These endpoints have no as_of
    parameter. The first pull is therefore the oldest snapshot that can exist.
    Nothing can rebuild the belief of the vendor from before that date.
    """
    if ds.key != "pull_date":
        raise ValueError(
            f"{ds.name} is an event stream. Use series(ds, start, end) or "
            f"on_date(ds, d)."
        )
    parts = partitions(ds)
    eligible = [p for p in parts if p <= as_of]
    if not eligible:
        earliest = f" The earliest pull is {parts[0]}." if parts else ""
        raise MissingPartition(
            f"{ds.name} has no pull at or before {as_of}.{earliest} This dataset "
            f"holds current state and you cannot backfill it. No snapshot of the "
            f"vendor belief on {as_of} exists, and you cannot obtain one."
        )
    return _read(ds, [eligible[-1]])


def snapshot_latest(ds: Dataset) -> duckdb.DuckDBPyRelation:
    """Return the most recent pull. This read is not point-in-time.

    The name makes the semantics clear at the call site. Use this function to
    explore, and to ask what the vendor says today. Do not use it for a number
    in a backtest.
    """
    parts = partitions(ds)
    if not parts:
        raise MissingPartition(_describe_coverage(ds))
    return _read(ds, [parts[-1]])


def history(ds: Dataset) -> duckdb.DuckDBPyRelation:
    """Return every pull, stacked, with the pull_date column.

    This read is not point-in-time. Use it to find restatements. A diff of
    complete snapshots is the only method available, because neither endpoint
    has an updated_since field. This relation is also the input to the dbt SCD
    Type 2 snapshot.
    """
    if ds.key != "pull_date":
        raise ValueError(f"{ds.name} is an event stream. Use series(ds, start, end).")
    parts = partitions(ds)
    if not parts:
        raise MissingPartition(_describe_coverage(ds))
    return _read(ds, parts)


# ---------- named accessors ----------

def day_aggs(start: dt.date | None = None, end: dt.date | None = None):
    """Return the daily OHLCV bars, one row for each ticker and session.

    The prices are unadjusted, and that is the purpose. The query applies the
    adjustment, so that the published partitions stay immutable. See the module
    docstring.
    """
    return series(DAY_AGGS, start, end)


def tickers(start: dt.date | None = None, end: dt.date | None = None):
    """Return the ticker reference data. The vendor rebuilds it for each date.

    This dataset is point-in-time. Ingest uses active=true only. A row for a
    date therefore means that the name was live and tradeable on that date.
    raw/ stores every instrument type. The filter on type='CS' and on the
    exchange is a modelling decision that dbt makes.
    """
    return series(TICKERS, start, end)


def tickers_on(d: dt.date):
    """Return the tradeable universe of one session."""
    return on_date(TICKERS, d)


def splits(as_of: dt.date):
    """Return the split snapshot as it was known on as_of. See snapshot()."""
    return snapshot(SPLITS, as_of)


def dividends(as_of: dt.date):
    """Return the dividend snapshot as it was known on as_of. See snapshot()."""
    return snapshot(DIVIDENDS, as_of)


def status() -> str:
    """Return one line for each dataset with its partition count and range."""
    lines = []
    for ds in DATASETS.values():
        parts = partitions(ds)
        if parts:
            lines.append(f"{ds.name:24s} {ds.key:10s} {len(parts):>5} partitions  "
                         f"{parts[0]} .. {parts[-1]}")
        else:
            lines.append(f"{ds.name:24s} {ds.key:10s}     0 partitions")
    return "\n".join(lines)


if __name__ == "__main__":
    print(status())

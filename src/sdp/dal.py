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
supplies the raw facts and the corporate action table. The join between them is
a modelling decision and belongs in SQL that a reader can see.

Reads name the partition files. They do not glob 'date=*'. A date filter
therefore removes files before the scan and not after it. Hive partitioning is
off, because build() already writes 'date' as a column in the file. The same
name from two sources gives ambiguity and no benefit.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Literal

import duckdb

from sdp.config import settings

PartitionKey = Literal["date"]
"""The partition key of an event stream. A current-state dataset has none."""


@dataclass(frozen=True)
class Dataset:
    """A published dataset in raw/.

    The 'key' field records the central data-model distinction. See
    docs/decisions/0002-partition-key-by-dataset-kind.md.

    'date'
        Event stream. The partition holds the facts of one session. It is
        immutable. You can backfill it. Read a range of dates with series().

    None
        Current state. One table, and no date in the path. The vendor gives
        its present belief about all of history and the endpoint has no
        as_of parameter. Each pull replaces the table. Read it with
        current(). See docs/decisions/0016-corporate-actions-are-current-state.md.
    """

    name: str
    key: PartitionKey | None
    union_by_name: bool = False
    """True for an event stream from REST that spans many files. read_json
    infers the schema of each pull, so a field the vendor added part way
    through a backfill makes a read without this flag fail. A current-state
    dataset is one file and has nothing to union."""

    @property
    def root(self):
        return settings.raw_dir / self.name

    @property
    def table_file(self):
        """The single file of a current-state dataset."""
        return self.root / "data.parquet"

    def partition_dir(self, d: dt.date):
        return self.root / f"{self.key}={d:%Y-%m-%d}"

    def partition_file(self, d: dt.date):
        return self.partition_dir(d) / "data.parquet"


DAY_AGGS = Dataset("us_stocks_day_aggs", "date")
TICKERS = Dataset("massive_tickers", "date", union_by_name=True)
SPLITS = Dataset("massive_splits", None)
DIVIDENDS = Dataset("massive_dividends", None)

# The two FINRA short datasets are event streams. For short interest the
# partition value is the settlement date. See docs/decisions/0009.
SHORT_VOLUME = Dataset("massive_short_volume", "date", union_by_name=True)
SHORT_INTEREST = Dataset("massive_short_interest", "date", union_by_name=True)

DATASETS = {d.name: d for d in (DAY_AGGS, TICKERS, SPLITS, DIVIDENDS,
                                SHORT_VOLUME, SHORT_INTEREST)}

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

def _require_event_stream(ds: Dataset, function: str) -> None:
    """Refuse a current-state dataset. It has no partition to describe.

    An empty list would be the wrong answer in the right shape. A current
    state table is published or it is not, and neither answer is a date.
    """
    if ds.key is None:
        raise ValueError(
            f"{ds.name} holds current state and has no date partition, so "
            f"{function}() has no answer for it. Use current(ds)."
        )


def partitions(ds: Dataset) -> list[dt.date]:
    """Return every published partition date, in ascending order.

    The list is empty when no partition exists.
    """
    _require_event_stream(ds, "partitions")
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
    _require_event_stream(ds, "coverage")
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
            f"{ds.name} holds current state and not an event stream. It has no "
            f"date partition. Use current(ds)."
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
            f"{ds.name} holds current state and has no date partition. "
            f"Use current(ds)."
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
    _require_event_stream(ds, "gaps")
    import exchange_calendars as xcals

    sessions = xcals.get_calendar("XNYS").sessions_in_range(
        start.isoformat(), end.isoformat()
    )
    have = set(partitions(ds))
    return [s.date() for s in sessions if s.date() not in have]


# ---------- current state: one table ----------

def current(ds: Dataset) -> duckdb.DuckDBPyRelation:
    """Return the corporate action table as the vendor states it now.

    The endpoint has no as_of parameter. It answers with the present belief
    of the vendor about all of history, so raw/ holds one table and each pull
    replaces it. There is no date in the path and no choice of pull to make
    at the call site.

    This read is not point-in-time and it is not meant to be. A study that
    needs the belief of the vendor on a past date must read the dated files
    in vendor/, which ingest keeps for every pull. See
    docs/decisions/0016-corporate-actions-are-current-state.md.
    """
    if ds.key is not None:
        raise ValueError(
            f"{ds.name} is an event stream. Use series(ds, start, end) or "
            f"on_date(ds, d)."
        )
    if not ds.table_file.exists():
        raise MissingPartition(
            f"{ds.name} is not published. Run python -m sdp.daily."
        )
    return con().read_parquet(str(ds.table_file), union_by_name=ds.union_by_name)


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


def splits():
    """Return the split table as the vendor states it now. See current()."""
    return current(SPLITS)


def dividends():
    """Return the dividend table as the vendor states it now. See current()."""
    return current(DIVIDENDS)


def short_volume(start: dt.date | None = None, end: dt.date | None = None):
    """Return the daily FINRA off-exchange short volume.

    'total_volume' here is FINRA off-exchange volume only. It is not
    consolidated tape volume and it is much smaller than 'volume' in the day
    aggregates. Never divide one dataset by the other. Coverage starts on
    2024-02-06, which is shorter than the window of the bars.
    """
    return series(SHORT_VOLUME, start, end)


def short_interest(start: dt.date | None = None, end: dt.date | None = None):
    """Return short interest, on a two-week cadence, keyed on the settlement date.

    The partition value is the settlement date and not the date on which FINRA
    published it. Publication comes about eight business days later. A study
    must apply that lag itself, in the staging model, or it uses a number before
    the market had it. See docs/decisions/0009.
    """
    return series(SHORT_INTEREST, start, end)


def status() -> str:
    """Return one line for each dataset with its partition count and range."""
    lines = []
    for ds in DATASETS.values():
        if ds.key is None:
            if ds.table_file.exists():
                n = con().sql(
                    f"select count(*) from read_parquet('{ds.table_file}')"
                ).fetchone()
                rows = n[0] if n else 0
                lines.append(f"{ds.name:24s} {'current':10s} {rows:>9} rows")
            else:
                lines.append(f"{ds.name:24s} {'current':10s} not published")
            continue
        parts = partitions(ds)
        if parts:
            lines.append(f"{ds.name:24s} {ds.key:10s} {len(parts):>5} partitions  "
                         f"{parts[0]} .. {parts[-1]}")
        else:
            lines.append(f"{ds.name:24s} {ds.key:10s}     0 partitions")
    return "\n".join(lines)


if __name__ == "__main__":
    print(status())

# src/sdp/dal.py
"""The only entry point for reads of the data lake. No other module opens a
Parquet file or a DuckDB database. Every lake accessor returns a lazy
DuckDBPyRelation. warehouse() returns a read-only connection to the dbt
warehouse. Price adjustment is a dbt model, not here.
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
    """A published dataset in raw/. key='date' is an event stream: one immutable
    partition per session, backfillable, read with series(). key=None is current
    state: one table with no date in the path, replaced by each pull, read with
    current()."""

    name: str
    key: PartitionKey | None
    union_by_name: bool = False
    """True for a multi-file REST event stream, so a field the vendor added mid
    backfill does not fail the read."""

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

# The FINRA short datasets are event streams. Short interest is keyed on the
# settlement date.
SHORT_VOLUME = Dataset("massive_short_volume", "date", union_by_name=True)
SHORT_INTEREST = Dataset("massive_short_interest", "date", union_by_name=True)

DATASETS = {d.name: d for d in (DAY_AGGS, TICKERS, SPLITS, DIVIDENDS,
                                SHORT_VOLUME, SHORT_INTEREST)}

_con: duckdb.DuckDBPyConnection | None = None


def con() -> duckdb.DuckDBPyConnection:
    """The shared in-memory connection that owns every relation. Two relations
    joined together must come from one connection."""
    global _con
    if _con is None:
        _con = duckdb.connect()
        _con.execute("set TimeZone = 'UTC'")
    return _con


class MissingPartition(FileNotFoundError):
    """The requested partition is not published."""


# ---------- partition discovery ----------

def _require_event_stream(ds: Dataset, function: str) -> None:
    """Refuse a current-state dataset. It has no partition to describe, and an
    empty list would be the wrong answer in the right shape."""
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
    """Rows of every published partition from start to end, both inclusive. A
    gap inside the range is not an error, because a failed audit leaves no
    partition. Use gaps() to find missing sessions."""
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
    """The XNYS sessions from start to end that have no partition. The calendar
    import is inside the function, because it is slow and most reads skip it."""
    _require_event_stream(ds, "gaps")
    import exchange_calendars as xcals

    sessions = xcals.get_calendar("XNYS").sessions_in_range(
        start.isoformat(), end.isoformat()
    )
    have = set(partitions(ds))
    return [s.date() for s in sessions if s.date() not in have]


# ---------- current state: one table ----------

def current(ds: Dataset) -> duckdb.DuckDBPyRelation:
    """The corporate action table as the vendor states it now. One table,
    replaced by each pull, so this read is not point-in-time. A past belief of
    the vendor comes from the dated files in vendor/."""
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
    """Daily OHLCV bars, one row per ticker and session. Unadjusted on purpose,
    so the published partitions stay immutable."""
    return series(DAY_AGGS, start, end)


def tickers(start: dt.date | None = None, end: dt.date | None = None):
    """Ticker reference data, point-in-time. A row for a date means the name was
    live and tradeable then. Instrument-type filtering is a dbt decision."""
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
    """Daily FINRA off-exchange short volume. total_volume here is FINRA
    off-exchange only, much smaller than the day-aggregate volume, so never
    divide one dataset by the other."""
    return series(SHORT_VOLUME, start, end)


def short_interest(start: dt.date | None = None, end: dt.date | None = None):
    """Short interest, keyed on the settlement date, not the publication date.
    A study must apply the ~8-session publication lag itself (the staging model
    does), or it reads a number before the market had it."""
    return series(SHORT_INTEREST, start, end)


def warehouse() -> duckdb.DuckDBPyConnection:
    """Return a read-only connection to the dbt warehouse. A build publishes a new file,
    so connect again to read a newer build."""
    path = settings.warehouse_path
    if not path.exists():
        raise MissingPartition(
            f"The warehouse is absent: {path}. Run python -m sdp.transform build."
        )
    return duckdb.connect(str(path), read_only=True)


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

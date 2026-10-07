"""Read access to the data lake and the dbt warehouse.

Lake accessors return lazy relations on one shared in-memory connection.
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

    key='date' is an event stream (one immutable partition per session, read with
    series()). key=None is current state (one table replaced by each pull, read with
    current()).
    """

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

# Short interest is keyed on the settlement date.
SHORT_VOLUME = Dataset("massive_short_volume", "date", union_by_name=True)
SHORT_INTEREST = Dataset("massive_short_interest", "date", union_by_name=True)

# One request per ticker, so one partition per month, on its last session.
TICKER_DETAILS = Dataset("massive_ticker_details", "date", union_by_name=True)

# The library restates the whole history in each file.
FRENCH = Dataset("french_factors", None)

# FRED revises values and serves a rolling window of some series, so the table folds
# the kept pulls.
FRED = Dataset("fred_series", None)

QFACTORS = Dataset("q_factors", None)

DATASETS = {d.name: d for d in (DAY_AGGS, TICKERS, SPLITS, DIVIDENDS, SHORT_VOLUME,
                                SHORT_INTEREST, TICKER_DETAILS, FRENCH, FRED, QFACTORS)}

_con: duckdb.DuckDBPyConnection | None = None


def con() -> duckdb.DuckDBPyConnection:
    """Return the shared in-memory connection that owns every relation.

    Relations that are joined must come from one connection.
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
    """Raise for a current-state dataset; an empty answer would look valid."""
    if ds.key is None:
        raise ValueError(
            f"{ds.name} holds current state and has no date partition, so "
            f"{function}() has no answer for it. Use current(ds)."
        )


def partitions(ds: Dataset) -> list[dt.date]:
    """Return every published partition date, ascending."""
    _require_event_stream(ds, "partitions")
    if not ds.root.exists():
        return []
    prefix = f"{ds.key}="
    found = []
    for p in ds.root.glob(f"{prefix}*"):
        try:
            d = dt.date.fromisoformat(p.name[len(prefix):])
        except ValueError:
            continue  # Not a partition directory.
        if ds.partition_file(d).exists():
            found.append(d)
    return sorted(found)


def coverage(ds: Dataset) -> tuple[dt.date, dt.date] | None:
    """Return the first and last published partition dates, or None."""
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
    """Return the rows of every published partition from start to end, inclusive.

    A gap inside the range is not an error: a failed audit leaves no partition.
    Use gaps() to find missing sessions.
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
    """Return one session of an event stream; raise MissingPartition if it is absent."""
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
    """Return the XNYS sessions from start to end that have no partition."""
    _require_event_stream(ds, "gaps")
    # Imported here: the calendar is slow to load and most reads skip it.
    import exchange_calendars as xcals

    sessions = xcals.get_calendar("XNYS").sessions_in_range(
        start.isoformat(), end.isoformat()
    )
    have = set(partitions(ds))
    return [s.date() for s in sessions if s.date() not in have]


# ---------- current state: one table ----------

def current(ds: Dataset) -> duckdb.DuckDBPyRelation:
    """Return a current-state table as the vendor states it now.

    Not point-in-time. A past belief of the vendor comes from the dated files in vendor/.
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
    """Return daily OHLCV bars, one row per ticker and session. Unadjusted."""
    return series(DAY_AGGS, start, end)


def tickers(start: dt.date | None = None, end: dt.date | None = None):
    """Return ticker reference data, point-in-time.

    A row for a date means the name was live and tradeable then.
    """
    return series(TICKERS, start, end)


def tickers_on(d: dt.date):
    """Return the tradeable universe of one session."""
    return on_date(TICKERS, d)


def splits():
    """Return the split table as the vendor states it now (see current())."""
    return current(SPLITS)


def dividends():
    """Return the dividend table as the vendor states it now (see current())."""
    return current(DIVIDENDS)


def ticker_details(start: dt.date | None = None, end: dt.date | None = None):
    """Return monthly ticker details, point-in-time.

    Columns include shares outstanding, market cap (whole company) and the SIC code.
    """
    return series(TICKER_DETAILS, start, end)


def french_factors():
    """Return the Fama-French daily factors as fractions. crsp_month names the library version."""
    return current(FRENCH)


def fred_series():
    """Return FRED series in long form: series_id, date, value, in FRED units.

    Rates and spreads are in percent. FRED publishes a date's value the next business
    day or later.
    """
    return current(FRED)


def q_factors():
    """Return the daily q5 factors as fractions. vintage names the file year."""
    return current(QFACTORS)


def short_volume(start: dt.date | None = None, end: dt.date | None = None):
    """Return daily FINRA off-exchange short volume.

    total_volume is off-exchange only, so never divide it by the day-aggregate volume.
    """
    return series(SHORT_VOLUME, start, end)


def short_interest(start: dt.date | None = None, end: dt.date | None = None):
    """Return short interest keyed on the settlement date, not the publication date.

    Apply the publication lag before use (int_short_interest does), or a study reads
    a number before the market had it.
    """
    return series(SHORT_INTEREST, start, end)


def warehouse() -> duckdb.DuckDBPyConnection:
    """Return a read-only connection to the dbt warehouse, in UTC.

    A build publishes a new file and DuckDB keeps one open database per path, so close
    every warehouse connection before connecting again to read the newer build.
    """
    path = settings.warehouse_path
    if not path.exists():
        raise MissingPartition(
            f"The warehouse is absent: {path}. Run python -m sdp.transform build."
        )
    wh = duckdb.connect(str(path), read_only=True)
    wh.execute("set TimeZone = 'UTC'")
    return wh


def status() -> str:
    """Return one line per dataset with its partition count and range."""
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

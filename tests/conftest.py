import datetime as dt

import duckdb
import pytest

from sdp import dal
from sdp.config import settings


@pytest.fixture
def tmp_data_root(tmp_path, monkeypatch):
    """Point settings.data_root at a temporary directory.

    The fixture patches data_root and not each directory property. The
    properties vendor_dir, raw_dir, staging_dir and warehouse_path all derive
    from data_root. This is the same reason that config.py anchors them to the
    repo root.
    """
    monkeypatch.setattr(settings, "data_root", tmp_path)
    return tmp_path


@pytest.fixture
def lake(tmp_data_root):
    """Return a helper that writes small partitions into the temporary lake."""

    def write(ds: dal.Dataset, d: dt.date, rows: list[tuple]):
        path = ds.partition_file(d)
        path.parent.mkdir(parents=True, exist_ok=True)
        values = ",".join(f"('{t}', {v}, date '{d:%Y-%m-%d}')" for t, v in rows)
        duckdb.execute(
            f"copy (select * from (values {values}) as t(ticker, value, {ds.key})) "
            f"to '{path}' (format parquet)"
        )
        return path

    return write


@pytest.fixture
def ca_lake(tmp_data_root, tmp_path):
    """Publish corporate action pulls through the real log-append path.

    The rows of one call are the full snapshot of that pull. append() reduces
    the snapshot to the change against the prior state, verifies the replay
    and publishes. The fixture therefore exercises the production write path
    and not a private copy of it.
    """
    from sdp.ingest import massive_corporate_actions as ca

    def write(ds: dal.Dataset, pull: dt.date, rows: list[tuple]):
        """rows are (id, ticker, event_date, factor)."""
        key = "execution_date" if ds is dal.SPLITS else "ex_dividend_date"
        staged = tmp_path / f"{ds.name}-{pull:%Y-%m-%d}.snapshot.parquet"
        if rows:
            values = ",".join(
                f"('{i}', '{t}', date '{d:%Y-%m-%d}', "
                f"{'null' if f is None else f})"
                for i, t, d, f in rows
            )
            select = (f"select * from (values {values}) as t"
                      f"(id, ticker, {key}, historical_adjustment_factor)")
        else:
            # An empty snapshot still needs the schema, so that append() can
            # hash it and close every prior row.
            select = (f"select null::varchar as id, null::varchar as ticker, "
                      f"null::date as {key}, "
                      f"null::double as historical_adjustment_factor "
                      f"where 1 = 0")
        duckdb.execute(f"copy ({select}) to '{staged}' (format parquet)")
        return ca.append(ds.name, staged, pull)

    return write


@pytest.fixture
def staged_parquet(tmp_path):
    """Write one staged Parquet file from a SQL select and return its path."""

    def write(name: str, select_sql: str):
        path = tmp_path / f"{name}.parquet"
        duckdb.execute(f"copy ({select_sql}) to '{path}' (format parquet)")
        return path

    return write

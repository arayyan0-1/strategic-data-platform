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

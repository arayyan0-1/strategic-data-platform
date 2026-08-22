import datetime as dt

import duckdb
import pytest

from sdp import dal
from sdp.config import settings


@pytest.fixture
def lake(tmp_path, monkeypatch):
    """Return a helper that writes partitions into a temporary data root.

    The fixture patches data_root and not each directory property. The
    properties vendor_dir, raw_dir and staging_dir all derive from data_root.
    This is the same reason that config.py anchors them to the repo root.
    """
    monkeypatch.setattr(settings, "data_root", tmp_path)

    def write(ds: dal.Dataset, d: dt.date, rows: list[tuple]):
        path = ds.partition_file(d)
        path.parent.mkdir(parents=True, exist_ok=True)
        key = ds.key
        values = ",".join(
            f"('{t}', {v}, date '{d:%Y-%m-%d}')" for t, v in rows
        )
        duckdb.execute(
            f"copy (select * from (values {values}) as t(ticker, value, {key})) "
            f"to '{path}' (format parquet)"
        )
        return path

    return write

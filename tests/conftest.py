import datetime as dt
import json
import os

import duckdb
import pytest

from sdp import dal
from sdp.config import settings


@pytest.fixture
def tmp_data_root(tmp_path, monkeypatch):
    """Point settings.data_root at a temporary directory.

    The properties vendor_dir, raw_dir, staging_dir and warehouse_path all
    derive from data_root, so patching it is sufficient.
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
    """Publish a corporate action table through the real ingest path.

    Each call replaces the table. The fixture goes through _publish, so it
    exercises the audits and the atomic replace.
    """
    from sdp.ingest import massive_corporate_actions as ca

    def write(ds: dal.Dataset, pull: dt.date, rows: list[tuple],
              *, audit: bool = False):
        """rows are (id, ticker, event_date, factor)."""
        key = "execution_date" if ds is dal.SPLITS else "ex_dividend_date"
        vendor = tmp_path / f"{ds.name}-{pull:%Y-%m-%d}.ndjson"
        vendor.write_text("".join(
            json.dumps({
                "id": i, "ticker": t, key: d.isoformat(),
                "historical_adjustment_factor": f,
            }) + "\n" for i, t, d, f in rows
        ), encoding="utf-8")

        if audit:
            return ca._publish(ds.name, vendor, pull)
        # Most tests care about the shape of the published table and not about
        # the audits, which have their own suite. Build and replace directly.
        staged = ca.build(ds.name, vendor, pull)
        dest = ca.raw_path(ds.name)
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged, dest)
        return dest

    return write


@pytest.fixture
def staged_parquet(tmp_path):
    """Write one staged Parquet file from a SQL select and return its path."""

    def write(name: str, select_sql: str):
        path = tmp_path / f"{name}.parquet"
        duckdb.execute(f"copy ({select_sql}) to '{path}' (format parquet)")
        return path

    return write

"""The rule that a rerun of one date yields identical rows.

This is a hard rule of the platform and no test stated it before. The rule is
about content and not about bytes. Parquet metadata and compression blocks can
differ between two runs, so these tests compare the rows and never the file.

The rule is what makes a backfill safe to run again. `--force` republishes a date
that already exists, and a rerun must not change what a published partition says.
"""
import datetime as dt
import gzip

import duckdb

from sdp.config import settings
from sdp.ingest import massive_day_aggs as day
from sdp.ingest import massive_tickers as tick

D = dt.date(2024, 1, 3)

CSV = """ticker,volume,open,close,high,low,window_start,transactions
AAA,1000.5,10.0,10.5,11.0,9.0,1704301200000000000,5
BBB,2000.0,20.0,20.5,21.0,19.0,1704301200000000000,7
CCC,3000.25,30.0,30.5,31.0,29.0,1704301200000000000,9
"""

NDJSON = (
    '{"ticker":"AAA","name":"Alpha","type":"CS","active":true,'
    '"primary_exchange":"XNYS","composite_figi":"BBG1"}\n'
    '{"ticker":"BBB","name":"Beta","type":"ETF","active":true,'
    '"primary_exchange":"XNAS","composite_figi":"BBG2"}\n'
)


def _rows(path):
    rel = duckdb.connect().read_parquet(str(path))
    return rel.order("ticker").fetchall()


def test_day_aggs_build_is_idempotent(tmp_data_root):
    vendor = day.vendor_path(D)
    vendor.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(vendor, "wt", encoding="utf-8") as fh:
        fh.write(CSV)

    first = _rows(day.build(D))
    second = _rows(day.build(D))
    assert first == second


def test_tickers_build_is_idempotent(tmp_data_root):
    """This failed while build() wrote now() as pulled_at."""
    vendor = settings.vendor_dir / tick.DATASET / "2024-01-03.ndjson"
    vendor.parent.mkdir(parents=True, exist_ok=True)
    vendor.write_text(NDJSON, encoding="utf-8")

    first = _rows(tick.build(vendor, D))
    second = _rows(tick.build(vendor, D))
    assert first == second


def test_the_tickers_build_writes_no_wall_clock_column(tmp_data_root):
    """A guard on the rule and not only on one run of it.

    A column from now() breaks idempotency. The partition key already carries
    the data date, so no such column is needed.
    """
    vendor = settings.vendor_dir / tick.DATASET / "2024-01-03.ndjson"
    vendor.parent.mkdir(parents=True, exist_ok=True)
    vendor.write_text(NDJSON, encoding="utf-8")

    rel = duckdb.connect().read_parquet(str(tick.build(vendor, D)))
    timestamps = [c for c, t in zip(rel.columns, rel.types, strict=True)
                  if "TIMESTAMP" in str(t)]
    assert timestamps == [], f"a wall-clock column breaks idempotency: {timestamps}"

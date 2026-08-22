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
    '"primary_exchange":"XNYS","composite_figi":"BBG1",'
    '"last_updated_utc":"2026-08-21T20:04:17Z"}\n'
    '{"ticker":"BBB","name":"Beta","type":"ETF","active":true,'
    '"primary_exchange":"XNAS","composite_figi":"BBG2",'
    '"last_updated_utc":"2026-08-21T20:04:17Z"}\n'
)
"""Every vendor record carries last_updated_utc. build() needs it to choose
between two copies of one ticker, so a fixture without it is not realistic."""


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
    # last_updated_utc is the revision stamp of the vendor record. It comes from
    # the vendor file and it is identical on every rebuild, so it is not a clock
    # reading taken at build time.
    timestamps = [c for c, t in zip(rel.columns, rel.types, strict=True)
                  if "TIMESTAMP" in str(t) and c != "last_updated_utc"]
    assert timestamps == [], f"a wall-clock column breaks idempotency: {timestamps}"


class TestVendorDuplicates:
    """The cursor pagination of the vendor can deliver one record twice.

    Two pulls of the same date give a different set of duplicates each time, so
    this is a transport artifact and not a property of the data. Measured on
    2026-08-21: one pull returned INIO, INKM, INKT, INLF and NET twice, and a
    second pull of the same date returned YINN and YJ twice instead.

    Without the removal, a ticker appears twice in the universe of that date and
    the audit refuses to publish the partition. Over 1,260 dates that fails
    often and at random.
    """

    DUPLICATED = (
        '{"ticker":"AAA","name":"Alpha","type":"CS","active":true,'
        '"last_updated_utc":"2026-08-21T20:04:17Z"}\n'
        '{"ticker":"AAA","name":"Alpha","type":"CS","active":true,'
        '"last_updated_utc":"2026-08-22T18:25:56Z"}\n'
        '{"ticker":"BBB","name":"Beta","type":"CS","active":true,'
        '"last_updated_utc":"2026-08-21T20:04:17Z"}\n'
    )

    def _vendor(self, name="2024-01-03.ndjson"):
        path = settings.vendor_dir / tick.DATASET / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.DUPLICATED, encoding="utf-8")
        return path

    def test_one_row_survives_for_each_ticker(self, tmp_data_root):
        rows = _rows(tick.build(self._vendor(), D))
        assert [r[0] for r in rows] == ["AAA", "BBB"]

    def test_the_newest_copy_is_kept(self, tmp_data_root):
        """read_json chooses the type of last_updated_utc, so compare and do not
        assume a string."""
        rel = duckdb.connect().read_parquet(str(tick.build(self._vendor(), D)))
        (kept,) = rel.filter("ticker = 'AAA'").project("last_updated_utc").fetchall()
        (newest,) = duckdb.connect().sql(
            "select max(last_updated_utc) from read_json("
            f"'{self._vendor()}', format='newline_delimited', sample_size=-1)"
        ).fetchone()
        assert kept[0] == newest

    def test_the_helper_column_does_not_reach_the_partition(self, tmp_data_root):
        rel = duckdb.connect().read_parquet(str(tick.build(self._vendor(), D)))
        assert "_copy" not in rel.columns

    def test_the_deduplicated_file_passes_the_duplicate_audit(self, tmp_data_root):
        """The audit stays fatal. It is now a backstop and not the first line."""
        staged = tick.build(self._vendor(), D)
        dupes = duckdb.connect().sql(
            f"select count(*) - count(distinct ticker) from read_parquet('{staged}')"
        ).fetchone()
        assert dupes == (0,)

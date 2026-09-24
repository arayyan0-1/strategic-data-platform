"""Audits and ingest modes of the two FINRA short datasets.

The vendor caps days_to_cover at 999.99. A row at the cap means "at or above
999.99", and the cause is not always a zero avg_daily_volume. The cap is not a
measurement, so the audit warns on it and does not fail.
"""
import datetime as dt
import gzip
import json
import logging

import pytest

from sdp import dal
from sdp.config import settings
from sdp.ingest import massive_short as ms

D = dt.date
DAY = D(2025, 3, 25)


def _volume_rows(rows: list[str]) -> str:
    return "select * from (values " + ",".join(rows) + ") as t(ticker, "\
        "total_volume, short_volume, exempt_volume, short_volume_ratio)"


def _v(ticker, total, short, exempt=0.0, ratio=None):
    ratio = 100.0 * short / total if ratio is None and total else (ratio or 0.0)
    return f"('{ticker}', {total}, {short}, {exempt}, {ratio})"


def _many_volume(n: int, **kw) -> list[str]:
    return [_v(f"T{i:05d}", 1000.0, 300.0) for i in range(n)]


def _interest_rows(rows: list[str]) -> str:
    return "select * from (values " + ",".join(rows) + ") as t(ticker, "\
        "settlement_date, short_interest, avg_daily_volume, days_to_cover)"


def _i(ticker, day=DAY, si=1000, adv=500, dtc=2.0):
    return f"('{ticker}', date '{day:%Y-%m-%d}', {si}, {adv}, {dtc})"


def _many_interest(n: int) -> list[str]:
    return [_i(f"T{i:05d}") for i in range(n)]


class TestShortVolume:
    def test_a_clean_file_passes(self, staged_parquet):
        p = staged_parquet("sv", _volume_rows(_many_volume(4000)))
        assert ms.audit_short_volume(p, DAY) == 4000

    def test_short_above_total_is_fatal(self, staged_parquet):
        rows = _many_volume(4000)
        rows.append(_v("BAD", 100.0, 900.0, ratio=900.0))
        p = staged_parquet("sv", _volume_rows(rows))
        with pytest.raises(ms.AuditFailure, match="above total_volume"):
            ms.audit_short_volume(p, DAY)

    def test_a_duplicate_ticker_is_fatal(self, staged_parquet):
        rows = _many_volume(4000)
        rows.append(_v("T00000", 1000.0, 300.0))
        p = staged_parquet("sv", _volume_rows(rows))
        with pytest.raises(ms.AuditFailure, match="duplicate tickers"):
            ms.audit_short_volume(p, DAY)

    def test_too_few_rows_is_fatal(self, staged_parquet):
        p = staged_parquet("sv", _volume_rows(_many_volume(10)))
        with pytest.raises(ms.AuditFailure, match="outside the limits"):
            ms.audit_short_volume(p, DAY)

    def test_a_fractional_total_volume_only_logs(self, staged_parquet, caplog):
        rows = _many_volume(4000)
        rows.append(_v("FRAC", 1000.5, 300.0))
        p = staged_parquet("sv", _volume_rows(rows))
        with caplog.at_level(logging.INFO):
            assert ms.audit_short_volume(p, DAY) == 4001
        assert "fractional total_volume" in caplog.text


class TestShortInterest:
    def test_a_clean_file_passes(self, staged_parquet):
        p = staged_parquet("si", _interest_rows(_many_interest(4000)))
        assert ms.audit_short_interest(p, DAY) == 4000

    def test_a_settlement_date_that_is_not_the_partition_is_fatal(
            self, staged_parquet):
        """The request named one settlement date.

        A different date in the answer puts those rows in the wrong partition,
        and a study then reads a number that belongs to another fortnight.
        """
        rows = _many_interest(4000)
        rows.append(_i("WRONG", day=D(2025, 3, 14)))
        p = staged_parquet("si", _interest_rows(rows))
        with pytest.raises(ms.AuditFailure, match="not 2025-03-25"):
            ms.audit_short_interest(p, DAY)

    @pytest.mark.parametrize("adv", [0, 5])
    def test_the_days_to_cover_cap_warns_and_does_not_fail(
            self, staged_parquet, caplog, adv):
        """The cap also occurs on a row with a non-zero avg_daily_volume."""
        rows = _many_interest(4000)
        rows.append(_i("ILLIQ", si=13823, adv=adv, dtc=999.99))
        p = staged_parquet("si", _interest_rows(rows))
        with caplog.at_level(logging.WARNING):
            assert ms.audit_short_interest(p, DAY) == 4001
        assert "sentinel" in caplog.text
        assert "unknown" in caplog.text

    def test_a_negative_short_interest_is_fatal(self, staged_parquet):
        rows = _many_interest(4000)
        rows.append(_i("NEG", si=-5))
        p = staged_parquet("si", _interest_rows(rows))
        with pytest.raises(ms.AuditFailure, match="negative short_interest"):
            ms.audit_short_interest(p, DAY)


class TestCoverageFloor:
    def test_short_volume_before_the_floor_publishes_nothing(self, tmp_data_root):
        """The endpoint has nothing before 2024-02-06, and that is not a failure."""
        assert ms.ingest_short_volume(D(2023, 6, 15)) is None

    def test_short_interest_before_the_floor_publishes_nothing(self, tmp_data_root):
        assert ms.ingest_short_interest(D(2016, 6, 15)) is None

    def test_a_day_that_is_not_a_session_publishes_nothing(self, tmp_data_root):
        assert ms.ingest_short_volume(D(2025, 3, 22)) is None


class TestBuild:
    def test_build_writes_the_partition_column(self, tmp_data_root, tmp_path):
        """The DAL reads without hive partitioning, so the file needs the column."""
        import duckdb
        vendor = tmp_path / "v.ndjson"
        vendor.write_text(
            '{"ticker":"AAA","settlement_date":"2025-03-25","short_interest":10,'
            '"avg_daily_volume":5,"days_to_cover":2.0}\n'
        )
        staged = ms.build(ms.SHORT_INTEREST, vendor, DAY)
        cols = [c[0] for c in duckdb.execute(
            f"describe select * from read_parquet('{staged}')").fetchall()]
        assert "date" in cols
        assert "settlement_date" in cols
        assert duckdb.execute(
            f"select date from read_parquet('{staged}')").fetchone()[0] == DAY

    def test_build_keeps_one_date_column_for_short_volume(
            self, tmp_data_root, tmp_path):
        import duckdb
        vendor = tmp_path / "v.ndjson"
        vendor.write_text(
            '{"ticker":"AAA","date":"2025-03-25","total_volume":10,'
            '"short_volume":3,"short_volume_ratio":30.0}\n'
        )
        staged = ms.build(ms.SHORT_VOLUME, vendor, DAY)
        cols = [c[0] for c in duckdb.execute(
            f"describe select * from read_parquet('{staged}')").fetchall()]
        assert cols.count("date") == 1


def _volume_vendor(n=4000):
    """Write a vendor NDJSON file that passes the short volume audit."""
    path = settings.vendor_dir / ms.SHORT_VOLUME / f"{DAY:%Y-%m-%d}.ndjson.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for i in range(n):
            fh.write(json.dumps({
                "ticker": f"T{i:05d}", "date": DAY.isoformat(),
                "total_volume": 1000.0, "short_volume": 300.0,
                "exempt_volume": 0.0, "short_volume_ratio": 30.0,
            }) + "\n")
    return path


@pytest.fixture
def offline(monkeypatch):
    """Make every download fail, so a test proves that a rebuild uses vendor/ only."""

    def fail(*args, **kwargs):
        raise AssertionError("a rebuild must not download")

    monkeypatch.setattr(ms, "dump_ndjson", fail)


INGEST = {ms.SHORT_VOLUME: ms.ingest_short_volume,
          ms.SHORT_INTEREST: ms.ingest_short_interest}


class TestModes:
    @pytest.mark.parametrize("dataset", sorted(INGEST))
    def test_rebuild_with_no_vendor_file_publishes_nothing(
            self, tmp_data_root, offline, dataset):
        """Most dates with no vendor file had no records, so this is not a failure."""
        assert INGEST[dataset](DAY, rebuild=True) is None
        assert not dal.DATASETS[dataset].partition_file(DAY).exists()

    @pytest.mark.parametrize("dataset", sorted(INGEST))
    def test_both_modes_raise(self, tmp_data_root, offline, dataset):
        with pytest.raises(ValueError, match="not set both"):
            INGEST[dataset](DAY, refetch=True, rebuild=True)

    def test_rebuild_republishes_a_published_partition(self, tmp_data_root, offline):
        import duckdb
        _volume_vendor()
        dest = ms.ingest_short_volume(DAY, rebuild=True)
        assert dest == dal.SHORT_VOLUME.partition_file(DAY)

        dest.write_bytes(b"damaged")
        assert ms.ingest_short_volume(DAY) == dest, "the default must skip it"
        assert ms.ingest_short_volume(DAY, rebuild=True) == dest
        assert duckdb.execute(
            f"select count(*) from read_parquet('{dest}')").fetchone() == (4000,)

    def test_refetch_downloads_again(self, tmp_data_root, monkeypatch):
        vendor = _volume_vendor()
        seen = []

        def dump_ndjson(dataset, path, params, pull_date, *, force=False, **kwargs):
            seen.append(force)
            return vendor

        monkeypatch.setattr(ms, "dump_ndjson", dump_ndjson)
        ms.ingest_short_volume(DAY)
        ms.ingest_short_volume(DAY, refetch=True)
        assert seen == [False, True]

    def test_a_refetch_with_no_records_keeps_the_partition(
            self, tmp_data_root, monkeypatch, caplog):
        _volume_vendor()
        monkeypatch.setattr(ms, "dump_ndjson", lambda *a, **k: None)
        dest = ms.ingest_short_volume(DAY, rebuild=True)
        with caplog.at_level(logging.WARNING):
            assert ms.ingest_short_volume(DAY, refetch=True) is None
        assert dest.exists()
        assert "stay" in caplog.text

    def test_a_refetch_with_no_records_keeps_the_vendor_file(
            self, tmp_data_root, monkeypatch, caplog):
        """An earlier pull that failed its audit leaves a vendor file and no partition."""
        vendor = _volume_vendor()
        before = vendor.read_bytes()
        monkeypatch.setattr(ms, "dump_ndjson", lambda *a, **k: None)
        with caplog.at_level(logging.WARNING):
            assert ms.ingest_short_volume(DAY, refetch=True) is None
        assert vendor.read_bytes() == before
        assert "no records" in caplog.text

    def test_rebuild_of_a_published_partition_with_no_vendor_file_raises(
            self, tmp_data_root, offline):
        """A published partition had records, so the missing file is an error."""
        vendor = _volume_vendor()
        ms.ingest_short_volume(DAY, rebuild=True)
        vendor.unlink()
        with pytest.raises(FileNotFoundError, match="does not exist"):
            ms.ingest_short_volume(DAY, rebuild=True)

    def test_a_refetch_that_fails_its_audit_keeps_the_earlier_vendor_bytes(
            self, tmp_data_root, monkeypatch):
        vendor = _volume_vendor()
        before = vendor.read_bytes()

        def dump_ndjson(dataset, path, params, pull_date, *, force=False, **kwargs):
            with gzip.open(vendor, "wt", encoding="utf-8") as fh:
                fh.write('{"ticker":"ONLY","date":"2025-03-25","total_volume":1.0,'
                         '"short_volume":0.0,'
                         '"exempt_volume":0.0,"short_volume_ratio":0.0}\n')
            return vendor

        monkeypatch.setattr(ms, "dump_ndjson", dump_ndjson)
        with pytest.raises(ms.AuditFailure):
            ms.ingest_short_volume(DAY, refetch=True)
        assert vendor.read_bytes() == before

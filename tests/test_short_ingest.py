"""Audits of the two FINRA short datasets.

The days_to_cover sentinel (999.99) warns but does not fail. The vendor writes
it when avg_daily_volume is 0, so reading it as a number inflates the least
liquid names.
"""
import datetime as dt
import logging

import pytest

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

    def test_the_days_to_cover_sentinel_warns_and_does_not_fail(
            self, staged_parquet, caplog):
        rows = _many_interest(4000)
        rows.append(_i("ILLIQ", si=13823, adv=0, dtc=999.99))
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

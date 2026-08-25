"""Point-in-time semantics of the read layer.

These tests are the ones that Python must do. The audits are data tests. They
check the output of the vendor. They say nothing about the code.

Partition selection is the step that can give wrong numbers in a correct shape.
A read of one pull too many adds lookahead, and no error occurs.
"""
import datetime as dt

import pytest

from sdp import dal

D = dt.date


class TestEventStreams:
    def test_series_reads_only_partitions_in_range(self, lake):
        for day in (D(2024, 1, 3), D(2024, 1, 4), D(2024, 1, 5)):
            lake(dal.DAY_AGGS, day, [("AAA", 1.0)])

        rel = dal.series(dal.DAY_AGGS, D(2024, 1, 4), D(2024, 1, 5))
        assert sorted(r[0] for r in rel.project("date").fetchall()) == [
            D(2024, 1, 4), D(2024, 1, 5)
        ]

    def test_series_bounds_default_to_published_extent(self, lake):
        lake(dal.DAY_AGGS, D(2024, 1, 3), [("AAA", 1.0)])
        lake(dal.DAY_AGGS, D(2024, 1, 5), [("AAA", 2.0)])
        assert dal.series(dal.DAY_AGGS).count("*").fetchone()[0] == 2

    def test_series_tolerates_a_gap_inside_the_range(self, lake):
        """A missing session is normal. A failed audit leaves no partition."""
        lake(dal.DAY_AGGS, D(2024, 1, 3), [("AAA", 1.0)])
        lake(dal.DAY_AGGS, D(2024, 1, 5), [("AAA", 2.0)])
        rel = dal.series(dal.DAY_AGGS, D(2024, 1, 3), D(2024, 1, 5))
        assert rel.count("*").fetchone()[0] == 2

    def test_series_refuses_a_current_state_dataset(self):
        with pytest.raises(ValueError, match="current state"):
            dal.series(dal.SPLITS)

    def test_on_date_names_the_missing_partition(self, lake):
        lake(dal.DAY_AGGS, D(2024, 1, 3), [("AAA", 1.0)])
        with pytest.raises(dal.MissingPartition, match="2024-01-04"):
            dal.on_date(dal.DAY_AGGS, D(2024, 1, 4))

    def test_gaps_lists_unpublished_sessions_only(self, lake):
        # 2024-01-03 to 2024-01-05 are XNYS sessions. 01-06 and 01-07 are a weekend.
        lake(dal.DAY_AGGS, D(2024, 1, 3), [("AAA", 1.0)])
        lake(dal.DAY_AGGS, D(2024, 1, 5), [("AAA", 2.0)])
        assert dal.gaps(dal.DAY_AGGS, D(2024, 1, 3), D(2024, 1, 7)) == [D(2024, 1, 4)]


class TestPartitionDiscovery:
    def test_partitions_ignores_a_directory_with_no_data_file(self, lake):
        lake(dal.DAY_AGGS, D(2024, 1, 3), [("AAA", 1.0)])
        (dal.DAY_AGGS.root / "date=2024-01-04").mkdir(parents=True)
        assert dal.partitions(dal.DAY_AGGS) == [D(2024, 1, 3)]

    def test_partitions_ignores_a_stray_directory(self, lake):
        lake(dal.DAY_AGGS, D(2024, 1, 3), [("AAA", 1.0)])
        (dal.DAY_AGGS.root / "date=_tmp").mkdir(parents=True)
        assert dal.partitions(dal.DAY_AGGS) == [D(2024, 1, 3)]

    def test_partitions_is_empty_when_nothing_is_ingested(self, lake):
        assert dal.partitions(dal.TICKERS) == []
        assert dal.coverage(dal.TICKERS) is None


class TestSchemaDrift:
    """The vendor can add a field during a five-year backfill."""

    def _write_wide(self, ds, d, extra_column: bool):
        import duckdb

        path = ds.partition_file(d)
        path.parent.mkdir(parents=True, exist_ok=True)
        extra = ", 'new' as vendor_added_field" if extra_column else ""
        duckdb.execute(
            f"copy (select 'AAA' as ticker, date '{d:%Y-%m-%d}' as {ds.key}{extra}) "
            f"to '{path}' (format parquet)"
        )

    def test_a_new_vendor_field_does_not_break_a_range_read(self, tmp_data_root):
        """This is the reason that union_by_name is on for the REST datasets."""
        self._write_wide(dal.TICKERS, D(2024, 1, 3), extra_column=False)
        self._write_wide(dal.TICKERS, D(2024, 1, 4), extra_column=True)

        rel = dal.series(dal.TICKERS)
        assert "vendor_added_field" in rel.columns
        assert rel.count("*").fetchone()[0] == 2

    def test_the_older_partition_carries_a_null_in_the_new_column(self, tmp_data_root):
        self._write_wide(dal.TICKERS, D(2024, 1, 3), extra_column=False)
        self._write_wide(dal.TICKERS, D(2024, 1, 4), extra_column=True)

        rows = dal.series(dal.TICKERS).order("date").project("vendor_added_field")
        assert rows.fetchall() == [(None,), ("new",)]


class TestSharedConnection:
    def test_two_relations_can_join(self, lake):
        """Every relation must belong to one connection, or a join raises."""
        lake(dal.DAY_AGGS, D(2024, 1, 3), [("AAA", 1.0)])
        lake(dal.TICKERS, D(2024, 1, 3), [("AAA", 2.0)])

        bars = dal.day_aggs()
        names = dal.tickers()
        joined = bars.join(names, "ticker")
        assert joined.count("*").fetchone()[0] == 1

    def test_con_returns_the_same_object_each_time(self):
        assert dal.con() is dal.con()


class TestMaterialisation:
    """A relation must convert to Python, not only to a DataFrame."""

    def test_a_timestamptz_column_reaches_python(self, tmp_data_root):
        """DuckDB needs pytz for this conversion. Without it, fetchall raises.

        day_aggs carries window_start_utc, so this is the normal case and not an
        edge case.
        """
        import duckdb

        path = dal.DAY_AGGS.partition_file(D(2024, 1, 3))
        path.parent.mkdir(parents=True, exist_ok=True)
        duckdb.execute(
            f"copy (select 'AAA' as ticker, date '2024-01-03' as date, "
            f"to_timestamp(1704301200) as window_start_utc) "
            f"to '{path}' (format parquet)"
        )
        (row,) = dal.day_aggs().project("window_start_utc").fetchall()
        assert row[0].tzinfo is not None

    def test_the_connection_renders_in_utc(self, tmp_data_root):
        """dal.con() sets TimeZone=UTC. A bare connection uses the system zone."""
        assert dal.con().sql("select current_setting('TimeZone')").fetchone() == ("UTC",)

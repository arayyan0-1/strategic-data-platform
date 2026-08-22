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

    def test_series_refuses_a_current_state_dataset(self, lake):
        lake(dal.SPLITS, D(2024, 1, 3), [("AAA", 1.0)])
        with pytest.raises(ValueError, match="pull_date"):
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


class TestCurrentStateSnapshots:
    """These tests cover the purpose of the pull_date partition key."""

    def test_snapshot_selects_the_newest_pull_at_or_before_as_of(self, lake):
        lake(dal.SPLITS, D(2024, 1, 3), [("AAA", 1.0)])
        lake(dal.SPLITS, D(2024, 1, 8), [("AAA", 2.0)])
        lake(dal.SPLITS, D(2024, 1, 15), [("AAA", 3.0)])

        rel = dal.snapshot(dal.SPLITS, D(2024, 1, 10))
        assert rel.fetchall() == [("AAA", 2.0, D(2024, 1, 8))]

    def test_snapshot_is_inclusive_of_a_pull_taken_on_as_of(self, lake):
        lake(dal.SPLITS, D(2024, 1, 3), [("AAA", 1.0)])
        lake(dal.SPLITS, D(2024, 1, 8), [("AAA", 2.0)])
        rel = dal.snapshot(dal.SPLITS, D(2024, 1, 8))
        assert rel.project("value").fetchall() == [(2.0,)]

    def test_snapshot_never_reads_a_later_pull(self, lake):
        """This is the lookahead that the module must prevent."""
        lake(dal.DIVIDENDS, D(2024, 1, 3), [("AAA", 1.0)])
        lake(dal.DIVIDENDS, D(2024, 6, 1), [("AAA", 99.0)])  # A restatement.

        rel = dal.snapshot(dal.DIVIDENDS, D(2024, 2, 1))
        assert 99.0 not in [r[0] for r in rel.project("value").fetchall()]

    def test_snapshot_returns_exactly_one_pull(self, lake):
        for day in (D(2024, 1, 3), D(2024, 1, 8), D(2024, 1, 15)):
            lake(dal.SPLITS, day, [("AAA", 1.0), ("BBB", 2.0)])
        rel = dal.snapshot(dal.SPLITS, D(2024, 1, 20))
        assert rel.count("*").fetchone()[0] == 2

    def test_snapshot_raises_before_the_first_pull(self, lake):
        """This is not a limit to avoid. That snapshot never existed."""
        lake(dal.SPLITS, D(2024, 1, 3), [("AAA", 1.0)])
        with pytest.raises(dal.MissingPartition, match="cannot backfill"):
            dal.snapshot(dal.SPLITS, D(2023, 12, 31))

    def test_snapshot_refuses_an_event_stream(self, lake):
        lake(dal.DAY_AGGS, D(2024, 1, 3), [("AAA", 1.0)])
        with pytest.raises(ValueError, match="event stream"):
            dal.snapshot(dal.DAY_AGGS, D(2024, 1, 3))

    def test_history_stacks_every_pull(self, lake):
        for day in (D(2024, 1, 3), D(2024, 1, 8), D(2024, 1, 15)):
            lake(dal.SPLITS, day, [("AAA", 1.0)])
        rel = dal.history(dal.SPLITS)
        assert rel.count("*").fetchone()[0] == 3
        assert rel.aggregate("count(distinct pull_date)").fetchone()[0] == 3

    def test_snapshot_latest_takes_the_newest(self, lake):
        lake(dal.SPLITS, D(2024, 1, 3), [("AAA", 1.0)])
        lake(dal.SPLITS, D(2024, 1, 15), [("AAA", 7.0)])
        assert dal.snapshot_latest(dal.SPLITS).project("value").fetchall() == [(7.0,)]


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

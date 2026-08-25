"""The contract of a current-state dataset.

Splits and dividends hold the belief of the vendor now. raw/ holds one table
for each, with no date in the path, and every pull replaces it. These tests
state what that guarantees and what it does not.

The thing it does not guarantee is a past belief. That is deliberate, and the
recovery path is vendor/, which keeps every pull. rebuild() reads it.
"""
import datetime as dt

import duckdb
import pytest

from sdp import dal
from sdp.config import settings
from sdp.ingest import massive_corporate_actions as ca

D = dt.date
P1, P2, P3 = D(2026, 8, 9), D(2026, 8, 15), D(2026, 8, 22)

ROW_A = ("a", "AAA", D(2024, 1, 5), 0.5)
ROW_B = ("b", "BBB", D(2024, 3, 8), 0.25)


def _rows(ds):
    return dal.current(ds).project(
        "id, historical_adjustment_factor").order("id").fetchall()


class TestTheTableIsCurrent:
    def test_a_pull_publishes_one_table_with_no_date_in_the_path(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A, ROW_B])

        assert dal.SPLITS.table_file == dal.SPLITS.root / "data.parquet"
        assert dal.SPLITS.table_file.exists()
        assert list(dal.SPLITS.root.glob("*=*")) == []

    def test_a_later_pull_replaces_the_table(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A, ROW_B])
        ca_lake(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.4)])

        assert _rows(dal.SPLITS) == [("a", 0.4)]

    def test_a_restated_factor_leaves_no_trace_of_the_old_value(self, ca_lake):
        """The cost of this storage, stated as a test.

        The published lake cannot answer what the vendor said before. Only
        vendor/ can, and TestRebuild covers that path.
        """
        ca_lake(dal.SPLITS, P1, [ROW_A])
        ca_lake(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.4)])

        factors = [f for _, f in _rows(dal.SPLITS)]
        assert factors == [0.4]
        assert 0.5 not in factors

    def test_the_table_records_which_vendor_pull_built_it(self, ca_lake):
        """Provenance and not a key. Nothing joins on it or filters by it."""
        ca_lake(dal.SPLITS, P2, [ROW_A])
        assert dal.current(dal.SPLITS).project(
            "vendor_pull_date").fetchall() == [(P2,)]

    def test_current_raises_before_the_first_pull(self, tmp_data_root):
        with pytest.raises(dal.MissingPartition, match="not published"):
            dal.current(dal.SPLITS)

    def test_the_named_accessors_take_no_as_of(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        ca_lake(dal.DIVIDENDS, P1, [ROW_B])
        assert dal.splits().aggregate("count(*)").fetchone() == (1,)
        assert dal.dividends().aggregate("count(*)").fetchone() == (1,)


class TestTheTwoKindsCannotBeConfused:
    """The guards that keep a current-state read out of an event stream."""

    def test_current_refuses_an_event_stream(self, lake):
        lake(dal.DAY_AGGS, D(2024, 1, 3), [("AAA", 1.0)])
        with pytest.raises(ValueError, match="event stream"):
            dal.current(dal.DAY_AGGS)

    def test_series_refuses_a_current_state_dataset(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        with pytest.raises(ValueError, match="current state"):
            dal.series(dal.SPLITS, D(2024, 1, 1), D(2024, 12, 31))

    def test_on_date_refuses_a_current_state_dataset(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        with pytest.raises(ValueError, match="current state"):
            dal.on_date(dal.SPLITS, P1)

    def test_the_short_datasets_are_event_streams(self):
        """A different answer from splits and dividends. See decision 0009.

        The endpoint takes the data date as a parameter, so the vendor can
        rebuild the answer for a past date.
        """
        assert dal.SHORT_VOLUME.key == "date"
        assert dal.SHORT_INTEREST.key == "date"
        with pytest.raises(ValueError, match="event stream"):
            dal.current(dal.SHORT_INTEREST)

    def test_the_backfill_refuses_a_current_state_target(self):
        from sdp import backfill

        with pytest.raises(SystemExit, match="no range to fill"):
            backfill.backfill("splits", D(2024, 1, 1), D(2024, 1, 5))


class TestRebuild:
    """vendor/ is the only path back to a past belief of the vendor."""

    def _vendor(self, pull: dt.date, factor: float):
        path = settings.vendor_dir / "massive_splits" / f"{pull:%Y-%m-%d}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            '{"id":"a","ticker":"AAA","execution_date":"2020-01-02",'
            '"split_from":1.0,"split_to":2.0,"adjustment_type":"forward_split",'
            f'"historical_adjustment_factor":{factor}}}\n',
            encoding="utf-8")
        return path

    def test_rebuild_uses_the_newest_pull_by_default(self, tmp_data_root):
        self._vendor(P1, 0.5)
        self._vendor(P3, 0.4)
        ca.rebuild("massive_splits")
        assert _rows(dal.SPLITS) == [("a", 0.4)]

    def test_rebuild_can_name_an_older_pull(self, tmp_data_root):
        """This is what replaces the point-in-time read of the old layout."""
        self._vendor(P1, 0.5)
        self._vendor(P3, 0.4)
        ca.rebuild("massive_splits", P1)
        assert _rows(dal.SPLITS) == [("a", 0.5)]
        assert dal.current(dal.SPLITS).project(
            "vendor_pull_date").fetchall() == [(P1,)]

    def test_rebuild_is_idempotent_at_the_level_of_content(self, tmp_data_root):
        self._vendor(P1, 0.5)
        first = duckdb.connect().read_parquet(
            str(ca.rebuild("massive_splits"))).order("id").fetchall()
        second = duckdb.connect().read_parquet(
            str(ca.rebuild("massive_splits"))).order("id").fetchall()
        assert first == second

    def test_rebuild_names_the_pulls_it_has_when_asked_for_a_missing_one(
        self, tmp_data_root
    ):
        self._vendor(P1, 0.5)
        with pytest.raises(FileNotFoundError, match="no vendor pull for 2026-08-22"):
            ca.rebuild("massive_splits", P3)

    def test_rebuild_without_vendor_files_refuses(self, tmp_data_root):
        with pytest.raises(FileNotFoundError, match="vendor"):
            ca.rebuild("massive_splits")


class TestThePartitionHelpersRefuseIt:
    """A current-state dataset has no partition, so these have no answer.

    An empty list or a list of missing dates would be the wrong answer in the
    right shape, which is the failure mode this platform cares about most.
    gaps() was the sharp one: it reported every session in the range as
    missing for a table that was published and complete.
    """

    @pytest.mark.parametrize("call, name", [
        (lambda: dal.partitions(dal.SPLITS), "partitions"),
        (lambda: dal.coverage(dal.SPLITS), "coverage"),
        (lambda: dal.gaps(dal.SPLITS, D(2026, 8, 3), D(2026, 8, 5)), "gaps"),
    ])
    def test_it_raises_and_names_the_read_to_use(self, ca_lake, call, name):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        with pytest.raises(ValueError, match="current"):
            call()

    def test_status_still_reports_a_current_state_dataset(self, ca_lake):
        """status() must not trip over the guard. It reports rows instead."""
        ca_lake(dal.SPLITS, P1, [ROW_A])
        line = [x for x in dal.status().split("\n") if "massive_splits" in x][0]
        assert "current" in line and "1 rows" in line

"""The daily driver.

Tests use a fake backfill and a fake ingest. No network.
"""
import datetime as dt

import pytest

from sdp import backfill, daily, dal

D = dt.date
TODAY = D(2024, 1, 10)


@pytest.fixture
def fake_backfill(monkeypatch):
    """Record the calls that the driver makes into sdp.backfill."""
    calls = []

    def fake(target, start, end, *, force=False, dry_run=False, limit=None):
        calls.append({"target": target, "start": start, "end": end, "limit": limit})
        return []

    monkeypatch.setattr(daily.backfill, "backfill", fake)
    return calls


@pytest.fixture
def fake_pull(monkeypatch):
    """Replace the corporate-action ingest with a recorder."""

    def install(fail: tuple = ()):
        calls = []

        def ingest(dataset, pull_date=None, *, force=False):
            calls.append(dataset)
            if dataset in fail:
                raise RuntimeError("vendor is down")
            return f"/fake/{dataset}"

        monkeypatch.setattr(daily.ca, "ingest", ingest)
        return calls

    return install


class TestEventStreamFill:
    def test_the_fill_starts_the_day_after_the_last_partition(self, lake, fake_backfill):
        lake(dal.DAY_AGGS, D(2024, 1, 4), [("AAA", 1.0)])
        daily._fill_event_stream("day_aggs", dal.DAY_AGGS, TODAY, 30)
        assert fake_backfill == [
            {"target": "day_aggs", "start": D(2024, 1, 5), "end": TODAY, "limit": 30}
        ]

    def test_an_up_to_date_stream_calls_no_backfill(self, lake, fake_backfill):
        lake(dal.DAY_AGGS, TODAY, [("AAA", 1.0)])
        assert daily._fill_event_stream("day_aggs", dal.DAY_AGGS, TODAY, 30) == []
        assert fake_backfill == []

    def test_a_cold_start_does_not_begin_a_full_backfill(self, tmp_data_root, fake_backfill):
        """An empty lake must not turn one daily run into a five-year job."""
        daily._fill_event_stream("day_aggs", dal.DAY_AGGS, TODAY, 30)
        (call,) = fake_backfill
        assert call["start"] == TODAY - dt.timedelta(days=daily.COLD_START_DAYS)

    def test_the_session_limit_reaches_the_runner(self, lake, fake_backfill):
        lake(dal.DAY_AGGS, D(2024, 1, 2), [("AAA", 1.0)])
        daily._fill_event_stream("day_aggs", dal.DAY_AGGS, TODAY, 3)
        assert fake_backfill[0]["limit"] == 3

    def test_a_range_with_no_session_calls_no_backfill(self, lake, fake_backfill):
        # 2024-01-06 is a Saturday. The next day is a Sunday.
        lake(dal.DAY_AGGS, D(2024, 1, 5), [("AAA", 1.0)])
        assert daily._fill_event_stream(
            "day_aggs", dal.DAY_AGGS, D(2024, 1, 7), 30
        ) == []
        assert fake_backfill == []

    def test_a_failure_is_returned_and_not_raised(self, lake, monkeypatch):
        lake(dal.DAY_AGGS, D(2024, 1, 4), [("AAA", 1.0)])
        monkeypatch.setattr(
            daily.backfill, "backfill",
            lambda *a, **k: [(D(2024, 1, 5), "RuntimeError: vendor is down")],
        )
        failures = daily._fill_event_stream("day_aggs", dal.DAY_AGGS, TODAY, 30)
        assert failures == [(D(2024, 1, 5), "RuntimeError: vendor is down")]


class TestCurrentStatePull:
    def test_both_datasets_are_pulled(self, tmp_data_root, fake_pull):
        calls = fake_pull()
        assert daily._pull_current_state(TODAY) == []
        assert calls == daily.CURRENT_STATE

    def test_one_failure_does_not_stop_the_other_dataset(self, tmp_data_root, fake_pull):
        calls = fake_pull(fail=("massive_splits",))
        failed = daily._pull_current_state(TODAY)
        assert failed == ["massive_splits"]
        assert "massive_dividends" in calls, "the second pull must still run"


class TestExitCode:
    def test_a_clean_run_returns_zero(self, lake, fake_backfill, fake_pull):
        fake_pull()
        assert daily.run(TODAY) == 0

    def test_a_failed_pull_returns_one(self, lake, fake_backfill, fake_pull):
        fake_pull(fail=("massive_splits",))
        assert daily.run(TODAY) == 1

    def test_a_failed_session_returns_one(self, lake, fake_pull, monkeypatch):
        lake(dal.DAY_AGGS, D(2024, 1, 4), [("AAA", 1.0)])
        fake_pull()
        monkeypatch.setattr(daily.backfill, "backfill",
                            lambda *a, **k: [(D(2024, 1, 5), "boom")])
        assert daily.run(TODAY) == 1

    def test_the_skip_flags_are_honoured(self, lake, fake_backfill, fake_pull):
        calls = fake_pull()
        lake(dal.DAY_AGGS, D(2024, 1, 4), [("AAA", 1.0)])
        daily.run(TODAY, skip_current_state=True)
        assert calls == []
        assert fake_backfill, "the event streams must still be filled"


def test_every_backfill_target_is_a_daily_event_stream():
    """The driver and the runner must agree on which datasets they fill."""
    assert set(daily.EVENT_STREAMS) == set(backfill.TARGETS)

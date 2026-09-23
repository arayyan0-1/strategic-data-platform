"""The daily driver.

Tests use a fake backfill and a fake ingest. No network.
"""
import datetime as dt
import json
import os

import pytest

from sdp import backfill, daily, dal
from sdp.config import settings

D = dt.date
TODAY = D(2024, 1, 10)
NOON = dt.datetime(2024, 1, 10, 12, 0, tzinfo=dt.UTC)


def _touch(path, mtime: float) -> None:
    """Create a file and set its mtime."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x", encoding="utf-8")
    os.utime(path, (mtime, mtime))


@pytest.fixture
def fake_backfill(monkeypatch):
    """Record the calls that the driver makes into sdp.backfill."""
    calls = []

    def fake(target, start, end, *, force=False, dry_run=False, limit=None,
             on_session=None):
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


class TestDbtStale:
    def test_an_absent_warehouse_is_stale(self, tmp_data_root):
        assert daily.dbt_stale() is True

    def test_a_warehouse_newer_than_raw_is_fresh(self, tmp_data_root):
        _touch(dal.DAY_AGGS.partition_file(D(2024, 1, 4)), 1000)
        _touch(settings.warehouse_path, 2000)
        assert daily.dbt_stale() is False

    def test_a_warehouse_older_than_raw_is_stale(self, tmp_data_root):
        _touch(settings.warehouse_path, 1000)
        _touch(dal.DAY_AGGS.partition_file(D(2024, 1, 4)), 2000)
        assert daily.dbt_stale() is True

    def test_an_empty_raw_is_not_stale(self, tmp_data_root):
        _touch(settings.warehouse_path, 1000)
        assert daily.dbt_stale() is False


class TestUpdate:
    @pytest.fixture
    def stub_run(self, monkeypatch):
        """Replace run() and the dbt build, so update() needs no network."""
        monkeypatch.setattr(daily, "run", lambda *a, **k: 0)
        built = []
        monkeypatch.setattr(daily, "_build_dbt",
                            lambda: (built.append(1), 0)[1])
        return built

    def test_it_writes_the_status_file(self, tmp_data_root, stub_run, monkeypatch):
        monkeypatch.setattr(daily, "dbt_stale", lambda: False)
        daily.update()
        status = json.loads((tmp_data_root / "_logs" / "status.json").read_text())
        assert status["last_pull"]["exit_code"] == 0
        assert status["last_pull"]["dbt"] == "skipped"

    def test_it_builds_only_when_stale(self, tmp_data_root, stub_run, monkeypatch):
        monkeypatch.setattr(daily, "dbt_stale", lambda: True)
        snap = daily.update()
        assert stub_run == [1]
        assert snap["last_pull"]["dbt"] == "built"

    def test_a_failed_dbt_build_is_reported(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(daily, "run", lambda *a, **k: 0)
        monkeypatch.setattr(daily, "dbt_stale", lambda: True)
        monkeypatch.setattr(daily, "_build_dbt", lambda: 1)
        snap = daily.update()
        assert snap["last_pull"]["dbt"] == "failed"

    def test_a_fresh_build_is_skipped(self, tmp_data_root, stub_run, monkeypatch):
        monkeypatch.setattr(daily, "dbt_stale", lambda: False)
        daily.update()
        assert stub_run == []

    def test_force_dbt_builds_a_fresh_warehouse(self, tmp_data_root, stub_run,
                                                monkeypatch):
        monkeypatch.setattr(daily, "dbt_stale", lambda: False)
        daily.update(force_dbt=True)
        assert stub_run == [1]

    def test_a_held_lock_blocks_a_second_update(self, tmp_data_root, stub_run):
        with daily._update_lock(), pytest.raises(daily.UpdateInProgress):
            daily.update()

    def test_a_stale_lock_is_reclaimed(self, tmp_data_root):
        lock = daily._lock_dir()
        lock.mkdir(parents=True)  # No pid file, so the lock reads as stale.
        with daily._update_lock():
            assert lock.exists()
        assert not lock.exists()

    def test_a_running_backfill_blocks_an_update(self, tmp_data_root, stub_run):
        b = daily._backfill_lock_dir()
        b.mkdir(parents=True)
        (b / "pid").write_text(str(os.getpid()))  # A live pid holds it.
        with pytest.raises(daily.UpdateInProgress):
            daily.update()

    def test_progress_counts_every_unit_to_the_total(self, lake, fake_pull,
                                                      monkeypatch):
        lake(dal.DAY_AGGS, D(2024, 1, 4), [("AAA", 1.0)])
        fake_pull()

        def fake_backfill(target, start, end, *, force=False, dry_run=False,
                          limit=None, on_session=None):
            for d in backfill.sessions(start, end)[:limit]:
                if on_session:
                    on_session(d, "ok")
            return []

        monkeypatch.setattr(daily.backfill, "backfill", fake_backfill)
        monkeypatch.setattr(daily, "dbt_stale", lambda: False)
        monkeypatch.setattr(daily, "_build_dbt", lambda: 0)

        events = []
        daily.update(TODAY, progress=events.append)

        totals = {e["total"] for e in events}
        assert len(totals) == 1, "the total must not move during the run"
        assert events[-1]["message"] == "Done"
        assert events[-1]["done"] == totals.pop()  # The bar reaches 100 percent.


class TestStatusSnapshot:
    def test_a_behind_stream_warns_and_counts(self, lake):
        lake(dal.DAY_AGGS, D(2024, 1, 4), [("AAA", 1.0)])
        row = _named(daily.status_snapshot(NOON), "day_aggs")
        assert row["behind"] and row["behind"] > 0
        assert row["level"] == "warn"

    def test_an_up_to_date_stream_is_ok(self, lake):
        expected = daily._expected_last_session(NOON)
        lake(dal.DAY_AGGS, expected, [("AAA", 1.0)])
        row = _named(daily.status_snapshot(NOON), "day_aggs")
        assert row["behind"] == 0
        assert row["level"] == "ok"

    def test_short_interest_never_reports_behind(self, lake):
        lake(dal.SHORT_INTEREST, D(2024, 1, 4), [("AAA", 1.0)])
        row = _named(daily.status_snapshot(NOON), "short_interest")
        assert row["behind"] is None
        assert row["level"] in ("ok", "warn")  # Sparse and lagged, so never bad.

    def test_each_dataset_reports_its_last_update_time(self, lake):
        lake(dal.DAY_AGGS, D(2024, 1, 4), [("AAA", 1.0)])
        row = _named(daily.status_snapshot(NOON), "day_aggs")
        assert row["updated"], "an event stream reports when its newest file wrote"


def _named(snap: dict, name: str) -> dict:
    return next(d for d in snap["datasets"] if d["name"] == name)

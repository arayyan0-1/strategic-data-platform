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

    def fake(target, start, end, *, refetch=False, rebuild=False, dry_run=False,
             limit=None, on_session=None):
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


@pytest.fixture(autouse=True)
def notifications(monkeypatch):
    """Stub the DNS probe and record the desktop notifications, so no test does a
    DNS lookup or shows an alert."""
    monkeypatch.setattr(daily, "_online", lambda: True)
    sent = []
    monkeypatch.setattr(daily, "_notify", sent.append)
    return sent


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

        def fake_backfill(target, start, end, *, refetch=False, rebuild=False,
                          dry_run=False, limit=None, on_session=None):
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


class TestOffline:
    @pytest.fixture
    def runs(self, monkeypatch):
        """Record the calls to run(), and make the vendor hosts not resolve."""
        calls = []
        monkeypatch.setattr(daily, "run", lambda *a, **k: calls.append(1) or 0)
        monkeypatch.setattr(daily, "dbt_stale", lambda: False)
        monkeypatch.setattr(daily, "_online", lambda: False)
        return calls

    def test_an_offline_run_skips_the_pulls(self, tmp_data_root, runs):
        snap = daily.update()
        assert runs == []
        assert snap["last_pull"]["offline"] is True
        assert snap["last_pull"]["exit_code"] == 0

    def test_an_offline_run_still_builds_a_stale_warehouse(self, tmp_data_root, runs,
                                                            monkeypatch):
        built = []
        monkeypatch.setattr(daily, "dbt_stale", lambda: True)
        monkeypatch.setattr(daily, "_build_dbt", lambda: built.append(1) or 0)
        assert daily.update()["last_pull"]["dbt"] == "built"
        assert built == [1]

    def test_main_exits_zero_when_offline(self, tmp_data_root, runs):
        assert daily.main([]) == 0
        status = json.loads((tmp_data_root / "_logs" / "status.json").read_text())
        assert status["last_pull"]["offline"] is True

    def test_an_online_run_records_offline_false(self, tmp_data_root, runs, monkeypatch):
        monkeypatch.setattr(daily, "_online", lambda: True)
        assert daily.update()["last_pull"]["offline"] is False
        assert runs == [1]

    def test_no_probe_when_both_pulls_are_skipped(self, tmp_data_root, runs, monkeypatch):
        monkeypatch.setattr(daily, "_online", lambda: pytest.fail("the probe ran"))
        snap = daily.update(skip_current_state=True, skip_event_streams=True)
        assert snap["last_pull"]["offline"] is False


class TestOnlineProbe:
    real = staticmethod(daily._online)  # Kept before the autouse stub replaces it.

    @pytest.fixture
    def resolver(self, monkeypatch):
        """Resolve only the hosts in a set, and record each lookup."""
        monkeypatch.setattr(settings, "massive_api_base", "https://api.example.test")
        monkeypatch.setattr(settings, "massive_s3_endpoint",
                            "https://files.example.test:8443")
        seen = []

        def install(resolves: set):
            def getaddrinfo(host, port):
                seen.append((host, port))
                if host not in resolves:
                    raise daily.socket.gaierror(
                        8, "nodename nor servname provided, or not known")
                return []

            monkeypatch.setattr(daily.socket, "getaddrinfo", getaddrinfo)
            return seen

        return install

    def test_no_host_resolves_is_offline(self, resolver):
        seen = resolver(set())
        assert self.real() is False
        assert seen == [("api.example.test", 443), ("files.example.test", 443)]

    def test_one_host_that_resolves_is_online(self, resolver):
        """A single bad host must fail its own pulls, not skip every pull."""
        resolver({"files.example.test"})
        assert self.real() is True


class TestFailureSignal:
    @pytest.fixture
    def codes(self, monkeypatch):
        """Make each run() return the next exit code of a list."""
        queue = []
        monkeypatch.setattr(daily, "run", lambda *a, **k: queue.pop(0))
        monkeypatch.setattr(daily, "dbt_stale", lambda: False)
        return queue

    def test_failures_in_a_row_counts_and_resets(self, tmp_data_root, codes):
        codes += [1, 1, 0]
        seen = [daily.update()["last_pull"]["failures_in_a_row"] for _ in range(3)]
        assert seen == [1, 2, 0]

    def test_an_offline_run_keeps_the_count(self, tmp_data_root, codes, monkeypatch):
        codes += [1, 1]
        daily.update()
        daily.update()
        monkeypatch.setattr(daily, "_online", lambda: False)
        assert daily.update()["last_pull"]["failures_in_a_row"] == 2

    def test_two_failures_send_one_notification_a_day(self, tmp_data_root, codes,
                                                       notifications):
        codes += [1, 1, 1]
        daily.update()
        assert notifications == [], "one failure must not alert"
        daily.update()
        daily.update()
        assert len(notifications) == 1
        today = dt.datetime.now(dt.UTC).date().isoformat()
        status = json.loads((tmp_data_root / "_logs" / "status.json").read_text())
        assert status["last_pull"]["last_alert"] == today

    def test_a_new_day_sends_a_new_notification(self, tmp_data_root, codes,
                                                notifications):
        codes += [1, 1, 1]
        daily.update()
        daily.update()
        p = tmp_data_root / "_logs" / "status.json"
        status = json.loads(p.read_text())
        status["last_pull"]["last_alert"] = "2000-01-01"
        p.write_text(json.dumps(status))
        daily.update()
        assert len(notifications) == 2

    def test_the_problems_are_recorded_up_to_20(self, tmp_data_root, monkeypatch):
        def failing_run(*a, problems=None, **k):
            problems.extend(f"day_aggs 2024-01-{i:02d}: boom" for i in range(1, 26))
            return 1

        monkeypatch.setattr(daily, "run", failing_run)
        monkeypatch.setattr(daily, "dbt_stale", lambda: False)
        problems = daily.update()["last_pull"]["problems"]
        assert len(problems) == 20
        assert problems[0] == "day_aggs 2024-01-01: boom"

    def test_run_exposes_its_problems(self, lake, fake_backfill, fake_pull):
        fake_pull(fail=("massive_splits",))
        problems = []
        assert daily.run(TODAY, problems=problems) == 1
        assert problems == ["massive_splits pull failed"]

    def test_a_failed_dbt_build_counts_as_a_failure(self, tmp_data_root, codes,
                                                    monkeypatch):
        codes += [0, 0]
        monkeypatch.setattr(daily, "dbt_stale", lambda: True)
        monkeypatch.setattr(daily, "_build_dbt", lambda: 1)
        daily.update()
        assert daily.update()["last_pull"]["failures_in_a_row"] == 2

    def test_the_dbt_failure_is_kept_under_the_problem_cap(self, tmp_data_root,
                                                           monkeypatch):
        def failing_run(*a, problems=None, **k):
            problems.extend(f"tickers 2024-01-{i:02d}: boom" for i in range(1, 26))
            return 1

        monkeypatch.setattr(daily, "run", failing_run)
        monkeypatch.setattr(daily, "dbt_stale", lambda: True)
        monkeypatch.setattr(daily, "_build_dbt", lambda: 1)
        assert "dbt build failed" in daily.update()["last_pull"]["problems"]

    def test_a_crash_in_the_run_counts_as_a_failure(self, tmp_data_root, monkeypatch):
        def crash(*a, **k):
            raise RuntimeError("corrupt partition")

        monkeypatch.setattr(daily, "run", crash)
        monkeypatch.setattr(daily, "dbt_stale", lambda: False)
        last_pull = daily.update()["last_pull"]
        assert last_pull["exit_code"] == 1
        assert last_pull["failures_in_a_row"] == 1
        assert "corrupt partition" in last_pull["problems"][0]

    def test_a_failed_snapshot_still_records_the_run(self, tmp_data_root, codes,
                                                     monkeypatch):
        codes += [1]

        def broken(*a, **k):
            raise OSError("unreadable partition")

        monkeypatch.setattr(daily, "status_snapshot", broken)
        daily.update()
        status = json.loads((tmp_data_root / "_logs" / "status.json").read_text())
        assert status["last_pull"]["failures_in_a_row"] == 1
        assert "unreadable partition" in status["error"]

    def test_a_run_that_pulls_nothing_keeps_the_count(self, tmp_data_root, codes):
        codes += [1, 1, 0]
        daily.update()
        daily.update()
        snap = daily.update(skip_current_state=True, skip_event_streams=True)
        assert snap["last_pull"]["failures_in_a_row"] == 2

    def test_an_offline_run_sends_no_failure_alert(self, tmp_data_root, codes,
                                                   notifications, monkeypatch):
        codes += [1, 1]
        daily.update()
        daily.update()
        p = tmp_data_root / "_logs" / "status.json"
        status = json.loads(p.read_text())
        status["last_pull"]["last_alert"] = "2000-01-01"
        p.write_text(json.dumps(status))
        monkeypatch.setattr(daily, "_online", lambda: False)
        daily.update()
        assert len(notifications) == 1, "only the second failure alerts"

    def test_offline_for_a_day_sends_one_alert(self, tmp_data_root, codes,
                                               notifications, monkeypatch):
        monkeypatch.setattr(daily, "_online", lambda: False)
        daily.update()
        assert notifications == []
        p = tmp_data_root / "_logs" / "status.json"
        status = json.loads(p.read_text())
        since = dt.datetime.now(dt.UTC) - dt.timedelta(hours=daily.OFFLINE_ALERT_HOURS + 1)
        status["last_pull"]["offline_since"] = since.isoformat()
        p.write_text(json.dumps(status))
        daily.update()
        daily.update()
        assert len(notifications) == 1
        assert "did not resolve" in notifications[0]

    def test_an_online_run_clears_offline_since(self, tmp_data_root, codes,
                                                monkeypatch):
        codes += [0]
        monkeypatch.setattr(daily, "_online", lambda: False)
        first = daily.update()["last_pull"]["offline_since"]
        assert first
        assert daily.update()["last_pull"]["offline_since"] == first
        monkeypatch.setattr(daily, "_online", lambda: True)
        assert daily.update()["last_pull"]["offline_since"] is None

    def test_the_status_file_rates_short_interest_on_this_run(self, tmp_data_root,
                                                              codes):
        codes += [0]
        daily.update()
        status = json.loads((tmp_data_root / "_logs" / "status.json").read_text())
        assert _named(status, "short_interest")["level"] == "ok"

    def test_a_failed_dbt_build_points_to_the_log(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(daily, "run", lambda *a, **k: 0)
        monkeypatch.setattr(daily, "dbt_stale", lambda: True)
        monkeypatch.setattr(daily, "_build_dbt", lambda: 1)
        events = []
        snap = daily.update(progress=events.append)
        assert "uv sync" not in events[-1]["message"]
        assert "daily.out.log" in events[-1]["message"]
        assert "dbt build failed" in snap["last_pull"]["problems"]


class TestNotify:
    real = staticmethod(daily._notify)  # Kept before the autouse stub replaces it.

    def test_an_osascript_error_is_swallowed(self, monkeypatch):
        def boom(*a, **k):
            raise FileNotFoundError("osascript")

        monkeypatch.setattr(daily.subprocess, "run", boom)
        self.real("The daily update failed.")

    def test_a_quote_in_the_message_is_escaped(self, monkeypatch):
        calls = []
        monkeypatch.setattr(daily.subprocess, "run", lambda cmd, **k: calls.append(cmd))
        self.real('a "b" c')
        (cmd,) = calls
        assert cmd[:2] == ["osascript", "-e"]
        assert cmd[2] == 'display notification "a \\"b\\" c" with title "sdp"'


class TestShortInterestLevel:
    @pytest.fixture
    def level(self, lake, tmp_data_root):
        """Write a last_pull block, then read the short interest level at NOON."""
        lake(dal.SHORT_INTEREST, D(2024, 1, 4), [("AAA", 1.0)])

        def read(**fields) -> str:
            p = tmp_data_root / "_logs" / "status.json"
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps({"last_pull": fields}), encoding="utf-8")
            return _named(daily.status_snapshot(NOON), "short_interest")["level"]

        return read

    def test_a_clean_pull_today_is_ok(self, level):
        assert level(time=NOON.isoformat(), exit_code=0) == "ok"

    def test_a_failed_pull_today_warns(self, level):
        assert level(time=NOON.isoformat(), exit_code=1) == "warn"

    def test_an_offline_run_today_warns(self, level):
        assert level(time=NOON.isoformat(), exit_code=0, offline=True) == "warn"

    def test_a_clean_pull_yesterday_warns(self, level):
        yesterday = NOON - dt.timedelta(days=1)
        assert level(time=yesterday.isoformat(), exit_code=0) == "warn"

    def test_no_pull_warns(self, level):
        assert level() == "warn"


class TestLogPruning:
    def test_an_old_backfill_log_is_deleted(self, tmp_data_root):
        logs = tmp_data_root / "_logs"
        now = dt.datetime.now().timestamp()
        old = logs / "backfill_day_aggs_20240101T000000.jsonl"
        new = logs / "backfill_day_aggs_20240201T000000.jsonl"
        _touch(old, now - (daily.LOG_KEEP_DAYS + 1) * 86400)
        _touch(new, now - 86400)
        daily._prune_logs()
        assert not old.exists()
        assert new.exists()

    def test_a_large_log_is_rotated(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(daily, "LOG_ROTATE_BYTES", 10)
        logs = tmp_data_root / "_logs"
        logs.mkdir(parents=True)
        (logs / "daily.out.log").write_text("x" * 11)
        (logs / "daily.out.log.1").write_text("older")
        (logs / "daily.err.log").write_text("small")
        daily._prune_logs()
        assert not (logs / "daily.out.log").exists()
        assert (logs / "daily.out.log.1").read_text() == "x" * 11
        assert (logs / "daily.err.log").read_text() == "small"
        assert not (logs / "daily.err.log.1").exists()

    def test_a_large_dashboard_log_is_copied_and_truncated(self, tmp_data_root,
                                                           monkeypatch):
        """The dashboard keeps its log open, so the file must stay in place."""
        monkeypatch.setattr(daily, "LOG_ROTATE_BYTES", 10)
        logs = tmp_data_root / "_logs"
        logs.mkdir(parents=True)
        log_file = logs / "dashboard.err.log"
        log_file.write_text("y" * 11)
        with log_file.open("a") as held:
            daily._prune_logs()
            held.write("next")
        assert (logs / "dashboard.err.log.1").read_text() == "y" * 11
        assert log_file.read_text() == "next"

    def test_update_prunes_the_logs(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(daily, "run", lambda *a, **k: 0)
        monkeypatch.setattr(daily, "dbt_stale", lambda: False)
        old = tmp_data_root / "_logs" / "backfill_tickers_20240101T000000.jsonl"
        _touch(old, 1000)
        daily.update()
        assert not old.exists()

    def test_a_pruning_error_does_not_raise(self, tmp_data_root, monkeypatch):
        old = tmp_data_root / "_logs" / "backfill_tickers_20240101T000000.jsonl"
        _touch(old, 1000)

        def deny(p, cutoff):
            raise PermissionError(p)

        monkeypatch.setattr(daily, "_prune_one", deny)
        daily._prune_logs()
        assert old.exists()


class TestFillEnd:
    def test_day_aggs_stops_at_the_last_published_session(self):
        # At noon on Wednesday 2024-01-10, Tuesday's file exists and today's does not.
        assert daily._fill_end("day_aggs", TODAY, NOON) == D(2024, 1, 9)

    def test_day_aggs_waits_for_the_morning_file(self):
        early = dt.datetime(2024, 1, 10, 5, 0, tzinfo=dt.UTC)
        assert daily._fill_end("day_aggs", TODAY, early) == D(2024, 1, 8)

    def test_the_rest_streams_fill_through_today(self):
        for name in ("tickers", "short_volume", "short_interest"):
            assert daily._fill_end(name, TODAY, NOON) == TODAY

    def test_a_past_date_is_not_moved(self):
        later = dt.datetime(2026, 9, 24, 12, 0, tzinfo=dt.UTC)
        assert daily._fill_end("day_aggs", TODAY, later) == TODAY

    def test_run_asks_for_no_unpublished_day_aggs_session(self, lake, fake_backfill,
                                                          fake_pull, monkeypatch):
        lake(dal.DAY_AGGS, D(2024, 1, 4), [("AAA", 1.0)])
        lake(dal.TICKERS, D(2024, 1, 4), [("AAA", 1.0)])
        fake_pull()
        monkeypatch.setattr(daily, "_expected_last_session", lambda now: D(2024, 1, 9))
        daily.run(TODAY)
        ends = {c["target"]: c["end"] for c in fake_backfill}
        assert ends["day_aggs"] == D(2024, 1, 9)
        assert ends["tickers"] == TODAY

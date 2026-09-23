"""The local status page.

Tests exercise the state builder and the single-writer guard. No socket, no
network: the handler's logic is in plain functions.
"""
import datetime as dt
import threading
import time

import pytest

from sdp import dal, dashboard

D = dt.date


@pytest.fixture(autouse=True)
def _reset_job():
    """The job state is a module global, so each test starts from idle."""
    idle = {"running": False, "phase": "idle", "started": None,
            "log": [], "result": None, "done": 0, "total": 0}
    dashboard._job.update(idle)
    yield
    dashboard._job.update(idle)


class TestState:
    def test_it_has_the_three_sections(self, tmp_data_root, lake):
        lake(dal.DAY_AGGS, D(2024, 1, 4), [("AAA", 1.0)])
        s = dashboard._state()
        assert "datasets" in s and "dbt" in s and "job" in s
        assert any(d["name"] == "day_aggs" for d in s["datasets"])

    def test_progress_events_reach_the_job(self):
        dashboard._progress({"message": "Filling day_aggs", "done": 2, "total": 8})
        assert dashboard._job["phase"] == "Filling day_aggs"
        assert dashboard._job["done"] == 2
        assert dashboard._job["total"] == 8

    def test_a_read_error_does_not_blank_the_page(self, monkeypatch):
        def boom():
            raise RuntimeError("lake is gone")
        monkeypatch.setattr(dashboard.daily, "status_snapshot", boom)
        s = dashboard._state()
        assert "lake is gone" in s["error"]
        assert s["job"]["running"] is False


class TestSingleFlight:
    def test_a_second_start_is_refused_while_one_runs(self, monkeypatch):
        gate = threading.Event()

        def slow_update(*a, **k):
            gate.wait(2)
            return {"last_pull": {"exit_code": 0, "dbt": "skipped"}}

        monkeypatch.setattr(dashboard.daily, "update", slow_update)

        assert dashboard._start_job() is True
        assert dashboard._start_job() is False  # One writer at a time.

        gate.set()
        for _ in range(100):
            if not dashboard._job["running"]:
                break
            time.sleep(0.02)
        assert dashboard._job["running"] is False
        assert dashboard._job["result"] == {"ok": True, "exit_code": 0,
                                            "dbt": "skipped"}

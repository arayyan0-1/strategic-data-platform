"""The local status page.

Tests exercise the state builder, the snapshot cache, the origin check and the
single-writer guard. No socket, no network: the handler's logic is in plain
functions.
"""
import datetime as dt
import errno
import http.client
import io
import threading
import time
from types import SimpleNamespace

import pytest

from sdp import dal, dashboard

D = dt.date


@pytest.fixture(autouse=True)
def _reset_job():
    """The job state and the snapshot cache are module globals, so each test
    starts from idle with no cached snapshot."""
    idle = {"running": False, "phase": "idle", "started": None,
            "log": [], "result": None, "done": 0, "total": 0}
    dashboard._job.update(idle)
    dashboard._clear_snapshot()
    yield
    dashboard._job.update(idle)
    dashboard._clear_snapshot()


def _wait_idle() -> None:
    for _ in range(250):
        if not dashboard._job["running"]:
            return
        time.sleep(0.02)
    pytest.fail("The job did not finish in 5 seconds.")


@pytest.fixture
def counted_snapshot(monkeypatch):
    """Replace the lake read with a counter. Return the list of calls."""
    calls = []

    def snap():
        calls.append(1)
        return {"datasets": [], "dbt": {}, "n": len(calls)}

    monkeypatch.setattr(dashboard.daily, "status_snapshot", snap)
    return calls


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
        _wait_idle()
        assert dashboard._job["running"] is False
        assert dashboard._job["result"] == {"ok": True, "exit_code": 0,
                                            "dbt": "skipped"}


class TestSnapshotCache:
    def test_two_polls_inside_the_ttl_read_the_lake_once(self, counted_snapshot):
        first = dashboard._state()
        second = dashboard._state()
        assert len(counted_snapshot) == 1
        assert first["n"] == second["n"] == 1

    def test_the_job_state_is_fresh_on_each_poll(self, counted_snapshot):
        dashboard._state()
        dashboard._progress({"message": "Filling day_aggs", "done": 3, "total": 9})
        s = dashboard._state()
        assert len(counted_snapshot) == 1
        assert s["job"]["done"] == 3 and s["job"]["phase"] == "Filling day_aggs"

    def test_an_expired_snapshot_is_read_again(self, counted_snapshot, monkeypatch):
        monkeypatch.setattr(dashboard, "SNAPSHOT_TTL", 0.0)
        dashboard._state()
        dashboard._state()
        assert len(counted_snapshot) == 2

    def test_a_read_error_is_not_cached(self, monkeypatch):
        calls = []

        def flaky():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("lake is busy")
            return {"datasets": [], "dbt": {}}

        monkeypatch.setattr(dashboard.daily, "status_snapshot", flaky)
        assert "lake is busy" in dashboard._state()["error"]
        assert "error" not in dashboard._state()
        assert len(calls) == 2

    def test_a_finished_job_clears_the_cache(self, counted_snapshot, monkeypatch):
        monkeypatch.setattr(dashboard.daily, "update", lambda *a, **k: {
            "last_pull": {"exit_code": 0, "dbt": "skipped"}})
        dashboard._state()
        assert dashboard._start_job() is True
        _wait_idle()
        s = dashboard._state()
        assert len(counted_snapshot) == 2
        assert s["n"] == 2 and s["job"]["result"]["ok"] is True


class TestOriginAndHost:
    PORT = 8787

    def test_an_absent_origin_is_allowed(self):
        assert dashboard._request_allowed({"Host": "127.0.0.1:8787"}, self.PORT)

    @pytest.mark.parametrize("host", ["127.0.0.1:8787", "localhost:8787"])
    def test_the_same_origin_is_allowed(self, host):
        headers = {"Host": host, "Origin": f"http://{host}"}
        assert dashboard._request_allowed(headers, self.PORT)

    @pytest.mark.parametrize("origin", ["https://example.com", "null",
                                        "http://127.0.0.1:9999",
                                        "https://127.0.0.1:8787"])
    def test_a_foreign_origin_is_refused(self, origin):
        headers = {"Host": "127.0.0.1:8787", "Origin": origin}
        assert not dashboard._request_allowed(headers, self.PORT)

    @pytest.mark.parametrize("host", ["evil.example:8787", "127.0.0.1:9999",
                                      "127.0.0.1", None])
    def test_a_wrong_host_is_refused(self, host):
        headers = {} if host is None else {"Host": host}
        assert not dashboard._request_allowed(headers, self.PORT)

    def test_parsed_headers_match_without_case(self):
        raw = b"host: localhost:8787\r\norigin: http://localhost:8787\r\n\r\n"
        headers = http.client.parse_headers(io.BytesIO(raw))
        assert dashboard._request_allowed(headers, self.PORT)

    def _handler(self, path, headers, sent):
        fake = SimpleNamespace(
            path=path, headers=headers,
            server=SimpleNamespace(server_address=("127.0.0.1", self.PORT)),
            _send=lambda code, body, *a: sent.append(code),
        )
        fake._refuse = lambda: dashboard.Handler._refuse(fake)
        return fake

    def test_a_refused_pull_does_not_start_a_job(self, monkeypatch):
        started = []
        monkeypatch.setattr(dashboard, "_start_job", lambda: started.append(1))
        sent = []
        headers = {"Host": "127.0.0.1:8787", "Origin": "https://example.com"}
        dashboard.Handler.do_POST(self._handler("/pull", headers, sent))
        assert sent == [403]
        assert started == []

    def test_a_get_with_a_wrong_host_is_refused(self, monkeypatch):
        monkeypatch.setattr(dashboard, "_state", lambda: pytest.fail("state was read"))
        sent = []
        headers = {"Host": "evil.example:8787"}
        dashboard.Handler.do_GET(self._handler("/state", headers, sent))
        assert sent == [403]

    def test_a_get_from_this_page_is_served(self, counted_snapshot):
        sent = []
        headers = {"Host": "localhost:8787"}
        dashboard.Handler.do_GET(self._handler("/state", headers, sent))
        assert sent == [200]


class TestSend:
    @pytest.mark.parametrize("exc", [BrokenPipeError(), ConnectionResetError(),
                                     ConnectionAbortedError(),
                                     OSError(errno.EPROTOTYPE, "Protocol wrong type")])
    def test_a_closed_connection_is_quiet(self, exc):
        def write(data):
            raise exc

        fake = SimpleNamespace(send_response=lambda code: None,
                               send_header=lambda k, v: None,
                               end_headers=lambda: None,
                               wfile=SimpleNamespace(write=write))
        dashboard.Handler._send(fake, 200, "{}")  # Must not raise.

    def test_a_different_os_error_is_raised(self):
        def write(data):
            raise OSError(errno.ENOSPC, "No space left on device")

        fake = SimpleNamespace(send_response=lambda code: None,
                               send_header=lambda k, v: None,
                               end_headers=lambda: None,
                               wfile=SimpleNamespace(write=write))
        with pytest.raises(OSError):
            dashboard.Handler._send(fake, 200, "{}")

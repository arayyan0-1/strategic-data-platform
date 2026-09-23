"""The vendor GET retries transport errors, HTTP 429 and HTTP 5xx, and stops at once on 4xx."""
import httpx
import pytest

from sdp.ingest import rest

BASE = "https://vendor.test"


@pytest.fixture
def sleeps(monkeypatch):
    """Record each backoff delay instead of a real sleep."""
    calls: list[float] = []
    monkeypatch.setattr(rest.time, "sleep", calls.append)
    return calls


def _client(*steps):
    """Return a client that answers each request with the next step, and the request list.

    A step is an httpx.Response, or an httpx exception class that the transport raises.
    """
    queue = list(steps)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        step = queue.pop(0)
        if isinstance(step, type) and issubclass(step, Exception):
            raise step("simulated failure", request=request)
        return step

    return httpx.Client(transport=httpx.MockTransport(handler), base_url=BASE), seen


def _ok(body=None):
    return httpx.Response(200, json=body if body is not None else {"ok": True})


class TestRetry:
    def test_a_503_then_a_200_succeeds(self, sleeps):
        client, seen = _client(httpx.Response(503), _ok())
        assert rest._get(client, "/p") == {"ok": True}
        assert len(seen) == 2
        assert sleeps == [1]

    def test_a_read_timeout_then_a_200_succeeds(self, sleeps):
        client, seen = _client(httpx.ReadTimeout, _ok())
        assert rest._get(client, "/p") == {"ok": True}
        assert len(seen) == 2
        assert sleeps == [1]

    @pytest.mark.parametrize("exc", [httpx.ConnectError, httpx.RemoteProtocolError])
    def test_other_transport_errors_retry(self, sleeps, exc):
        client, _ = _client(exc, exc, _ok())
        assert rest._get(client, "/p") == {"ok": True}
        assert sleeps == [1, 2]

    def test_the_backoff_is_exponential_and_capped(self, sleeps):
        client, _ = _client(*[httpx.Response(500)] * 7, _ok())
        rest._get(client, "/p", attempts=8)
        assert sleeps == [1, 2, 4, 8, 16, 30, 30]


class TestRetryAfter:
    @pytest.mark.parametrize("status", [429, 503])
    def test_an_integer_retry_after_sets_the_delay(self, sleeps, status):
        client, _ = _client(httpx.Response(status, headers={"Retry-After": "7"}), _ok())
        rest._get(client, "/p")
        assert sleeps == [7]

    def test_a_large_retry_after_is_capped(self, sleeps):
        client, _ = _client(httpx.Response(429, headers={"Retry-After": "600"}), _ok())
        rest._get(client, "/p")
        assert sleeps == [120]

    @pytest.mark.parametrize("value", ["Wed, 21 Oct 2026 07:28:00 GMT", "1.5", "-3", ""])
    def test_a_retry_after_that_is_not_an_integer_uses_the_backoff(self, sleeps, value):
        client, _ = _client(httpx.Response(429, headers={"Retry-After": value}), _ok())
        rest._get(client, "/p")
        assert sleeps == [1]

    def test_retry_after_on_a_502_is_ignored(self, sleeps):
        client, _ = _client(httpx.Response(502, headers={"Retry-After": "7"}), _ok())
        rest._get(client, "/p")
        assert sleeps == [1]


class TestFailure:
    def test_exhausted_retries_name_the_url_the_count_and_the_last_error(self, sleeps):
        client, seen = _client(httpx.Response(503), httpx.ReadTimeout, httpx.ConnectError)
        with pytest.raises(RuntimeError) as info:
            rest._get(client, "/p", {"date": "2024-03-04"}, attempts=3)
        msg = str(info.value)
        assert "All 3 attempts on https://vendor.test/p?date=2024-03-04 failed." in msg
        assert "The last error was ConnectError: simulated failure." in msg
        assert len(seen) == 3
        assert sleeps == [1, 2], "no sleep after the last attempt"

    def test_exhausted_retries_on_status_name_the_status(self, sleeps):
        client, _ = _client(*[httpx.Response(504)] * 2)
        with pytest.raises(RuntimeError, match="The last error was HTTP 504."):
            rest._get(client, "/p", attempts=2)
        assert sleeps == [1]

    def test_a_404_fails_at_once(self, sleeps):
        client, seen = _client(httpx.Response(404, text="no such endpoint"), _ok())
        with pytest.raises(RuntimeError) as info:
            rest._get(client, "/p")
        assert "HTTP 404" in str(info.value)
        assert "no such endpoint" in str(info.value)
        assert len(seen) == 1
        assert sleeps == []


class TestPaginate:
    def test_paginate_follows_next_url_through_a_retry(self, sleeps, monkeypatch):
        next_url = f"{BASE}/p?cursor=abc"
        client, seen = _client(
            _ok({"results": [{"i": 0}, {"i": 1}], "next_url": next_url}),
            httpx.Response(502),
            httpx.RemoteProtocolError,
            _ok({"results": [{"i": 2}]}),
        )
        monkeypatch.setattr(rest, "_client", lambda: client)

        assert [r["i"] for r in rest.paginate("/p", {"limit": 2})] == [0, 1, 2]
        assert [str(r.url) for r in seen] == [
            f"{BASE}/p?limit=2", next_url, next_url, next_url,
        ]
        assert sleeps == [1, 2]

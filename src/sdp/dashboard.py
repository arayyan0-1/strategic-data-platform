"""Local status server for the data lake and the market monitor.

    python -m sdp.dashboard [--port PORT]

It only listens on 127.0.0.1. A pull started from the page runs in a background thread.
"""
from __future__ import annotations

import argparse
import datetime as dt
import errno
import json
import logging
import threading
import time
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from sdp import daily, market, stock

log = logging.getLogger("sdp.dashboard")

HOST = "127.0.0.1"
PORT = 8787
SNAPSHOT_TTL = 30.0  # Seconds. A snapshot reads footers and sizes across the lake.
MAX_BODY = 16 * 1024  # Bytes. A portfolio request of 60 lines is under 4 KB.

_job_lock = threading.Lock()
_job: dict = {"running": False, "phase": "idle", "started": None,
              "log": [], "result": None, "done": 0, "total": 0}

_snap_lock = threading.Lock()
_snap_cache: dict = {"at": 0.0, "snap": None}


def _progress(ev: dict) -> None:
    with _job_lock:
        msg = ev.get("message") or ""
        _job["phase"] = msg
        if msg:
            _job["log"] = (_job["log"] + [msg])[-40:]
        if ev.get("total") is not None:
            _job["done"] = ev.get("done") or 0
            _job["total"] = ev["total"]


def _run_job(force_dbt: bool) -> None:
    try:
        snap = daily.update(force_dbt=force_dbt, progress=_progress)
        code = snap["last_pull"]["exit_code"]
        dbt = snap["last_pull"]["dbt"]
        offline = bool(snap["last_pull"].get("offline"))
        result = {"ok": code == 0 and dbt != "failed" and not offline,
                  "exit_code": code, "dbt": dbt, "offline": offline}
    except daily.UpdateInProgress as exc:
        result = {"ok": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 -- the page must show any failure
        log.exception("The update failed.")
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    _clear_snapshot()  # Before the job ends, so a poll that sees the end reads the lake.
    with _job_lock:
        _job["result"] = result
        _job["running"] = False
        _job["phase"] = "idle"


def _start_job(force_dbt: bool = False) -> bool:
    """Start a pull in the background. Return False when one already runs."""
    with _job_lock:
        if _job["running"]:
            return False
        _job.update(running=True, phase="starting",
                    started=dt.datetime.now(dt.UTC).isoformat(),
                    log=[], result=None, done=0, total=0)
    threading.Thread(target=_run_job, args=(force_dbt,), daemon=True).start()
    return True


def _snapshot() -> dict:
    """Return the status snapshot, cached for SNAPSHOT_TTL seconds.

    The lock lets one thread read the lake while the others wait for its result.
    """
    with _snap_lock:
        now = time.monotonic()
        if _snap_cache["snap"] is None or now - _snap_cache["at"] >= SNAPSHOT_TTL:
            _snap_cache["snap"] = daily.status_snapshot()
            _snap_cache["at"] = now
        return dict(_snap_cache["snap"])


def _clear_snapshot() -> None:
    """Drop the cached snapshot so the next poll reads the lake again."""
    with _snap_lock:
        _snap_cache["snap"] = None


def _state() -> dict:
    """Return the snapshot and the job.

    The job is read first, so a finished job always comes with a snapshot taken after it.
    """
    with _job_lock:
        job = {"running": _job["running"], "phase": _job["phase"],
               "started": _job["started"], "log": list(_job["log"]),
               "result": _job["result"],
               "done": _job.get("done", 0), "total": _job.get("total", 0)}
    try:
        snap = _snapshot()
    except Exception as exc:  # noqa: BLE001 -- a read error must not blank the page
        snap = {"error": f"{type(exc).__name__}: {exc}", "datasets": [], "dbt": {}}
    snap["job"] = job
    return snap


def _host_allowed(headers: Mapping[str, str], port: int) -> bool:
    """Return True when the Host header names this server (blocks DNS rebinding)."""
    host = (headers.get("Host") or "").lower()
    return host in {f"127.0.0.1:{port}", f"localhost:{port}"}


def _request_allowed(headers: Mapping[str, str], port: int) -> bool:
    """Return True when a POST comes from this page; the Origin check blocks other sites."""
    if not _host_allowed(headers, port):
        return False
    origin = headers.get("Origin")
    return origin is None or origin.lower() in {
        f"http://127.0.0.1:{port}", f"http://localhost:{port}"}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:  # noqa: D102 -- silence the access log
        pass

    def _send(self, code: int, body: str, ctype: str = "application/json") -> None:
        data = body.encode("utf-8")
        try:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            # The data changes with each build, so the browser must not keep a copy.
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)
        except ConnectionError:
            return  # The client closed the connection.
        except OSError as exc:
            if exc.errno != errno.EPROTOTYPE:  # macOS gives this for a closing socket.
                raise

    def _refuse(self) -> None:
        self._send(403, json.dumps({"error": "the request is not from this page"}))

    def do_GET(self) -> None:  # noqa: N802 -- the base class names it
        if not _host_allowed(self.headers, self.server.server_address[1]):
            self._refuse()
        elif self.path in ("/", "/index.html") or self.path.startswith("/?"):
            self._send(200, PAGE, "text/html; charset=utf-8")
        elif self.path == "/state":
            self._send(200, json.dumps(_state()))
        elif self.path == "/market" or self.path.startswith("/market?"):
            self._send(200, market.PAGE, "text/html; charset=utf-8")
        elif self.path == "/market.json":
            try:
                self._send(200, json.dumps(market.snapshot()))
            except Exception as exc:  # noqa: BLE001 -- the page must show any failure
                log.exception("The market payload failed.")
                self._send(500, json.dumps({"errors": [f"{type(exc).__name__}: {exc}"]}))
        elif self.path.startswith("/market/"):
            self._view()
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def _view(self) -> None:
        """Serve the search, stock and pair views of sdp.stock."""
        url = urlsplit(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        routes = {"/market/search.json": ("search",),
                  "/market/stock.json": ("profile", q.get("t", "")),
                  "/market/pair.json": ("pair", q.get("a", ""), q.get("b", ""))}
        if url.path not in routes:
            self._send(404, json.dumps({"error": "not found"}))
            return
        try:
            self._send(200, json.dumps(stock.view(*routes[url.path]), allow_nan=False))
        except stock.UnknownTicker as exc:
            self._send(404, json.dumps({"error": str(exc)}))
        except Exception as exc:  # noqa: BLE001 -- the page must show any failure
            log.exception("The view %s failed.", self.path)
            self._send(500, json.dumps({"error": f"{type(exc).__name__}: {exc}"}))

    def _portfolio(self) -> None:
        """Serve the portfolio view. The body is {"holdings": [{"ticker", "amount"}]}.

        A failure logs the error type only, so the log never holds the holdings.
        """
        try:
            size = int(self.headers.get("Content-Length") or "")
        except ValueError:
            self._send(400, json.dumps({"error": "the request needs a Content-Length"}))
            return
        if size < 0:
            self._send(400, json.dumps({"error": "the request needs a Content-Length"}))
        elif size > MAX_BODY:
            self._send(413, json.dumps({"error": f"the request is over {MAX_BODY} bytes"}))
        else:
            try:
                body = json.loads(self.rfile.read(size))
                holdings = body["holdings"]
            except (ValueError, KeyError, TypeError, RecursionError):
                self._send(400, json.dumps({"error": "the body must be JSON with holdings"}))
                return
            try:
                self._send(200, json.dumps(stock.portfolio_view(holdings), allow_nan=False))
            except stock.BadHoldings as exc:
                self._send(400, json.dumps({"error": str(exc)}))
            except Exception as exc:  # noqa: BLE001 -- the page must show any failure
                log.error("The portfolio view failed: %s", type(exc).__name__)
                self._send(500, json.dumps(
                    {"error": f"the portfolio view failed: {type(exc).__name__}"}))

    def do_POST(self) -> None:  # noqa: N802
        if not _request_allowed(self.headers, self.server.server_address[1]):
            self._refuse()
        elif self.path == "/market/portfolio.json":
            self._portfolio()
        elif self.path == "/pull":
            if _start_job():
                self._send(202, json.dumps({"started": True}))
            else:
                self._send(409, json.dumps({"started": False,
                                            "error": "an update is already running"}))
        else:
            self._send(404, json.dumps({"error": "not found"}))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m sdp.dashboard",
                                description="Serve the local status page.")
    p.add_argument("--port", type=int, default=PORT,
                   help=f"The port of the server. The default is {PORT}.")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    server = ThreadingHTTPServer((HOST, args.port), Handler)
    print(f"sdp dashboard on http://{HOST}:{server.server_address[1]}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


# The page lives in its own file so editors treat it as HTML.
PAGE = Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")

if __name__ == "__main__":
    raise SystemExit(main())

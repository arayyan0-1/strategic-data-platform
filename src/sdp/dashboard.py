# src/sdp/dashboard.py
"""A local status page for the data lake.

    python -m sdp.dashboard

Serves http://127.0.0.1:8787. The page shows what each dataset holds, what is
missing, and how old the dbt build is. One button pulls the missing sessions and
builds the dbt models. The page is local only and does not need the network,
except when it pulls. The pull runs in a background thread, so the page stays
live while it works.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from sdp import daily

log = logging.getLogger("sdp.dashboard")

HOST = "127.0.0.1"
PORT = 8787

_job_lock = threading.Lock()
_job: dict = {"running": False, "phase": "idle", "started": None,
              "log": [], "result": None, "done": 0, "total": 0}


# ---------- the background pull ----------

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
        result = {"ok": code == 0 and dbt != "failed",
                  "exit_code": code, "dbt": dbt}
    except daily.UpdateInProgress as exc:
        result = {"ok": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 -- the page must show any failure
        log.exception("The update failed.")
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
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


def _state() -> dict:
    try:
        snap = daily.status_snapshot()
    except Exception as exc:  # noqa: BLE001 -- a read error must not blank the page
        snap = {"error": f"{type(exc).__name__}: {exc}", "datasets": [], "dbt": {}}
    with _job_lock:
        snap["job"] = {"running": _job["running"], "phase": _job["phase"],
                       "started": _job["started"], "log": list(_job["log"]),
                       "result": _job["result"],
                       "done": _job.get("done", 0), "total": _job.get("total", 0)}
    return snap


# ---------- the server ----------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args) -> None:  # noqa: D102 -- silence the access log
        pass

    def _send(self, code: int, body: str, ctype: str = "application/json") -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:  # noqa: N802 -- the base class names it
        if self.path in ("/", "/index.html") or self.path.startswith("/?"):
            self._send(200, PAGE, "text/html; charset=utf-8")
        elif self.path == "/state":
            self._send(200, json.dumps(_state()))
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/pull":
            if _start_job():
                self._send(202, json.dumps({"started": True}))
            else:
                self._send(409, json.dumps({"started": False,
                                            "error": "an update is already running"}))
        else:
            self._send(404, json.dumps({"error": "not found"}))


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"sdp dashboard on http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>sdp data status</title>
<style>
  :root { --ok:#1a7f37; --warn:#9a6700; --bad:#cf222e; --bg:#f6f8fa;
          --card:#fff; --line:#d0d7de; --ink:#1f2328; --dim:#656d76; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--ink);
         font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
  .wrap { max-width:760px; margin:0 auto; padding:28px 20px 60px; }
  h1 { font-size:20px; margin:0 0 2px; }
  .sub { color:var(--dim); font-size:13px; margin-bottom:20px; }
  .bar { display:flex; align-items:center; gap:14px; margin-bottom:22px; }
  button { font:600 15px inherit; color:#fff; background:#1f6feb; border:0;
           border-radius:8px; padding:11px 20px; cursor:pointer; }
  button:disabled { background:#8c959f; cursor:default; }
  .phase { color:var(--dim); font-size:13px; }
  table { width:100%; border-collapse:collapse; background:var(--card);
          border:1px solid var(--line); border-radius:10px; overflow:hidden; }
  th,td { text-align:left; padding:11px 14px; border-bottom:1px solid var(--line); }
  th { font-size:12px; text-transform:uppercase; letter-spacing:.04em;
       color:var(--dim); background:#f0f3f6; }
  tr:last-child td { border-bottom:0; }
  .name { font-weight:600; }
  .dot { display:inline-block; width:9px; height:9px; border-radius:50%;
         margin-right:8px; vertical-align:middle; }
  .ok .dot { background:var(--ok); }
  .warn .dot { background:var(--warn); }
  .bad .dot { background:var(--bad); }
  .ok .behind { color:var(--ok); }
  .warn .behind { color:var(--warn); }
  .bad .behind { color:var(--bad); }
  .detail { color:var(--dim); font-size:13px; }
  td.updated { color:var(--dim); font-size:13px; white-space:nowrap; text-align:right; }
  #progress { margin:-8px 0 22px; }
  .pbar { height:8px; background:#e6e9ec; border-radius:6px; overflow:hidden; }
  .pbar > i { display:block; height:100%; width:0; background:#1f6feb; transition:width .3s; }
  .pmeta { display:flex; justify-content:space-between; gap:12px;
           color:var(--dim); font-size:13px; margin-top:6px; }
  .sec { margin:26px 0 8px; font-size:12px; text-transform:uppercase;
         letter-spacing:.04em; color:var(--dim); }
  .banner { border-radius:8px; padding:10px 14px; margin-bottom:16px; font-size:14px; }
  .banner.bad { background:#ffebe9; color:var(--bad); }
  .banner.ok { background:#dafbe1; color:var(--ok); }
  pre { background:#0d1117; color:#c9d1d9; border-radius:8px; padding:12px 14px;
        font-size:12px; overflow:auto; max-height:160px; margin:10px 0 0; }
</style>
</head>
<body>
<div class="wrap">
  <h1>sdp data status</h1>
  <div class="sub" id="generated">reading…</div>
  <div class="bar">
    <button id="pull" onclick="pull()">Update everything</button>
    <span class="phase" id="phase"></span>
  </div>
  <div id="progress" hidden>
    <div class="pbar"><i id="pfill"></i></div>
    <div class="pmeta"><span id="pmsg"></span><span id="peta"></span></div>
  </div>
  <div id="banner"></div>
  <div id="body"></div>
  <pre id="logbox" hidden></pre>
</div>
<script>
function rel(iso) {
  const t = Date.parse(iso);
  if (isNaN(t)) return "";
  const s = (Date.now() - t) / 1000;
  if (s < 90) return "just now";
  if (s < 3600) return Math.round(s / 60) + "m ago";
  if (s < 86400) return Math.round(s / 3600) + "h ago";
  return Math.round(s / 86400) + "d ago";
}
function fmtDur(s) {
  s = Math.max(0, Math.round(s));
  if (s < 60) return s + "s";
  return Math.round(s / 60) + " min";
}
function eta(j) {
  if (j.done >= j.total) return "finishing…";
  if (!j.started || !j.done) return "estimating…";
  const elapsed = (Date.now() - Date.parse(j.started)) / 1000;
  return "~" + fmtDur(elapsed * (j.total - j.done) / j.done) + " left";
}
function row(d) {
  const behind = d.behind == null ? "" :
      (d.behind === 0 ? "up to date" : d.behind + " behind");
  const updated = d.updated ? rel(d.updated) : "";
  return `<tr class="${d.level}">
    <td class="name"><span class="dot"></span>${d.name}</td>
    <td class="detail">${d.detail || ""}</td>
    <td class="updated">${updated}</td>
    <td class="behind">${behind}</td></tr>`;
}
function render(s) {
  const gen = s.generated ? new Date(s.generated).toLocaleString() : "";
  let head = "checked " + gen;
  if (s.expected_last_session) head += " · newest session " + s.expected_last_session;
  document.getElementById("generated").textContent = head;

  let banner = "";
  const j = s.job || {};
  if (j.result && !j.running) {
    const dbt = j.result.dbt;
    if (j.result.ok) {
      const note = dbt === "built" ? "dbt models rebuilt."
                 : dbt === "skipped" ? "dbt already current." : "";
      banner = `<div class="banner ok">Update finished. ${note}</div>`;
    } else if (dbt === "failed") {
      banner = `<div class="banner bad">Pull done, but the dbt build failed. `
             + `Run: uv sync --group transform</div>`;
    } else {
      const why = j.result.error || ("exit code " + j.result.exit_code);
      banner = `<div class="banner bad">Update problem: ${why}</div>`;
    }
  }
  if (s.error) banner = `<div class="banner bad">Read error: ${s.error}</div>`;
  document.getElementById("banner").innerHTML = banner;

  const ds = s.datasets || [];
  const events = ds.filter(d => d.kind === "event" || d.kind === "sparse");
  const current = ds.filter(d => d.kind === "current");
  const dbt = s.dbt || {};
  const sec = (t, inner) => `<div class="sec">${t}</div><table><tbody>${inner}</tbody></table>`;
  const dbtInfo = {name:"dbt build", level:dbt.level||"bad", detail:dbt.detail||"", behind:null};
  document.getElementById("body").innerHTML =
    sec("Market data", events.map(row).join(""))
    + sec("Corporate actions", current.map(row).join(""))
    + sec("Derived tables (dbt)", row(dbtInfo));

  const btn = document.getElementById("pull");
  btn.disabled = !!j.running;

  // The progress bar shows once the plan has a total; before that, a word.
  const prog = document.getElementById("progress");
  if (j.running && j.total > 0) {
    prog.hidden = false;
    const pct = Math.min(100, Math.round(j.done / j.total * 100));
    document.getElementById("pfill").style.width = pct + "%";
    document.getElementById("pmsg").textContent =
      (j.phase || "") + " · " + j.done + "/" + j.total;
    document.getElementById("peta").textContent = eta(j);
  } else {
    prog.hidden = true;
  }
  document.getElementById("phase").textContent =
    (j.running && j.total === 0) ? (j.phase || "starting…") : "";

  const logbox = document.getElementById("logbox");
  if (j.running && j.log && j.log.length) {
    logbox.hidden = false; logbox.textContent = j.log.join("\\n");
  } else { logbox.hidden = true; }
}
let timer = null;
async function poll() {
  let running = false;
  try {
    const s = await (await fetch("/state")).json();
    render(s);
    running = !!(s.job && s.job.running);
  } catch (e) {}
  clearTimeout(timer);
  timer = setTimeout(poll, running ? 500 : 2000);  // Poll fast while a job runs.
}
async function pull() {
  document.getElementById("pull").disabled = true;
  try {
    const r = await fetch("/pull", {method:"POST"});
    if (r.status === 409) alert("An update is already running.");
  } catch (e) { alert("Could not start the update: " + e); }
  clearTimeout(timer);
  timer = setTimeout(poll, 150);  // Start polling at once, so the bar appears.
}
poll();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    raise SystemExit(main())

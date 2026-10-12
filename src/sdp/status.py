"""The status of every source, the pipeline and the warehouse, for the status page.

Each dataset is described by a catalog entry (provider, cadence, what it holds) and measured
from the lake: coverage, gaps, rows of recent partitions, freshness against its own rule,
what is due next, and its size in raw/ and vendor/. Reads file footers, not data, wherever
it can, so a snapshot takes well under a second.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from dataclasses import dataclass
from pathlib import Path

import duckdb

from sdp import dal
from sdp.config import settings


@dataclass(frozen=True)
class Source:
    name: str
    group: str
    title: str
    provider: str
    cadence: str
    what: str
    model: str  # The staging model that reads it.
    unit: str  # What one row counts.
    ds: dal.Dataset


GROUPS = {
    "Market data": "Massive (formerly Polygon.io), Stocks Starter: about five years of daily "
                   "history, rolling.",
    "Corporate actions": "Massive. Current state: each pull replaces the table, and vendor/ "
                         "keeps the dated pulls.",
    "Public data": "Public sources with no key. Each pulls when its table reaches its age.",
    "Funding and ETFs": "Borrow from IBKR and fund data from iShares and SSGA. Only SSGA's NAV "
                        "history can be refetched; a missed day is lost for the rest.",
}

CATALOG = [
    Source("day_aggs", "Market data", "Daily bars", "Massive S3 flat files",
           "Each session; the file lands about 06:00 UTC the next day",
           "OHLCV of every US ticker that traded, unadjusted.",
           "stg_massive__day_aggs", "tickers", dal.DAY_AGGS),
    Source("tickers", "Market data", "Ticker reference", "Massive REST /v3/reference/tickers",
           "Each session, on the day",
           "Active tickers with type, exchange, FIGI and CIK, as stated that day.",
           "stg_massive__tickers", "tickers", dal.TICKERS),
    Source("short_volume", "Market data", "Short volume", "Massive REST (FINRA)",
           "Each session, from 2024-02-06",
           "Off-exchange short volume per ticker. No model reads it yet.",
           "(source only)", "tickers", dal.SHORT_VOLUME),
    Source("short_interest", "Market data", "Short interest", "Massive REST (FINRA)",
           "Twice a month; public about eight sessions after settlement",
           "Shares short, average volume and days to cover, per settlement date.",
           "stg_massive__short_interest", "tickers", dal.SHORT_INTEREST),
    Source("ticker_details", "Market data", "Ticker details", "Massive REST",
           "Each month-end session",
           "Shares outstanding, market cap and SIC code per common stock.",
           "stg_massive__ticker_details", "tickers", dal.TICKER_DETAILS),
    Source("splits", "Corporate actions", "Splits", "Massive REST /stocks/v1/splits",
           "Daily, weekends included", "Every split the vendor knows of, past and announced.",
           "stg_massive__splits", "splits", dal.SPLITS),
    Source("dividends", "Corporate actions", "Dividends", "Massive REST /stocks/v1/dividends",
           "Daily, weekends included",
           "Every dividend the vendor knows of. Pulls older than 30 days are thinned to weekly.",
           "stg_massive__dividends", "dividends", dal.DIVIDENDS),
    Source("fred_series", "Public data", "FRED series", "St. Louis Fed, one CSV per series",
           "Daily", "Treasury curve, TIPS, breakevens, credit, money markets, the Fed balance "
           "sheet, currencies and commodities. Folds every kept pull.",
           "stg_fred__series", "observations", dal.FRED),
    Source("french_factors", "Public data", "Fama-French factors", "Kenneth French Data Library",
           "Weekly; the library lags about two months",
           "Five factors, momentum, reversal and 12 industries, daily since 1963.",
           "stg_french__factors", "days", dal.FRENCH),
    Source("french_international", "Public data", "French international and US history",
           "Kenneth French Data Library", "Weekly; the library lags about two months",
           "Monthly returns of seven regions and 21 countries in dollars, and US size, value, "
           "momentum, profitability and variance portfolios since 1926.",
           "stg_french__international", "monthly returns", dal.FRENCH_INTERNATIONAL),
    Source("q_factors", "Public data", "q-factors", "global-q.org",
           "Monthly; a new vintage about once a year", "The q5 factors, daily since 1967.",
           "stg_global_q__factors", "days", dal.QFACTORS),
    Source("ibkr_borrow", "Funding and ETFs", "Borrow fees", "IBKR FTP (ftp2), usa.txt",
           "One snapshot per session from 08:00 ET",
           "Fee, rebate and shares available per US symbol.",
           "stg_ibkr__borrow", "symbols", dal.IBKR_BORROW),
    Source("etf_funds", "Funding and ETFs", "ETF screeners", "iShares and SSGA websites",
           "One snapshot per US Eastern date",
           "NAV, net assets and asset class of every iShares and SSGA ETF.",
           "stg_issuers__etf_funds", "funds", dal.ETF_FUNDS),
    Source("etf_nav_history", "Funding and ETFs", "SSGA NAV history", "SSGA fund data files",
           "Every 30 days; fills the days the screener missed",
           "Daily NAV, shares outstanding and net assets per SSGA ETF since inception.",
           "stg_ssga__nav_history", "fund-days", dal.ETF_NAV_HISTORY),
    Source("etf_holdings", "Funding and ETFs", "ETF holdings", "iShares and SSGA websites",
           "Daily per fund; a fund not yet updated is tried again after 8 hours",
           "Holdings files of the funds in the seed etf_holdings_funds.",
           "stg_issuers__etf_holdings", "lines", dal.ETF_HOLDINGS),
]

HISTORY_PARTITIONS = 30
GAP_SESSIONS = 60


# ---------- small measures ----------

def _iso(d) -> str | None:
    return d.isoformat() if d else None


def _mtime(path: Path) -> str | None:
    try:
        return dt.datetime.fromtimestamp(path.stat().st_mtime, dt.UTC).isoformat()
    except OSError:
        return None


def _size(root: Path) -> tuple[int, int]:
    """Return the bytes and file count under a directory, 0 when it is absent."""
    total = files = 0
    stack = [root]
    while stack:
        try:
            entries = list(os.scandir(stack.pop()))
        except OSError:
            continue
        for e in entries:
            if e.is_dir(follow_symlinks=False):
                stack.append(Path(e.path))
            elif e.is_file(follow_symlinks=False):
                total += e.stat(follow_symlinks=False).st_size
                files += 1
    return total, files


def _row_counts(files: list[Path]) -> dict[str, int]:
    """Return rows per file from the Parquet footers."""
    if not files:
        return {}
    names = ", ".join(f"'{f}'" for f in files)
    return {name: n for name, n in duckdb.sql(
        f"select file_name, num_rows from parquet_file_metadata([{names}])").fetchall()}


def _one(sql: str):
    try:
        return duckdb.sql(sql).fetchone()
    except duckdb.Error:
        return None


# ---------- per-kind measures ----------

def _event(src: Source, expected: dt.date | None, out: dict) -> list[dt.date]:
    """Fill coverage, recent rows and gaps of an event stream; return its partitions."""
    ds = src.ds
    parts = dal.partitions(ds)
    out["first"], out["last"] = (_iso(parts[0]), _iso(parts[-1])) if parts else (None, None)
    out["partitions"] = len(parts)
    recent = parts[-HISTORY_PARTITIONS:]
    if ds.per_member:
        out["history"] = [[_iso(d), len(ds.files(d))] for d in recent]
        newest = ds.files(parts[-1]) if parts else []
        rows = sum(_row_counts(newest).values()) if newest else None
        out["latest_rows"] = rows
    else:
        counts = _row_counts([ds.partition_file(d) for d in recent])
        out["history"] = [[_iso(d), counts.get(str(ds.partition_file(d)))] for d in recent]
        if out["history"]:
            out["latest_rows"] = out["history"][-1][1]
            if len(out["history"]) > 1 and out["history"][-2][1]:
                out["latest_change"] = out["latest_rows"] / out["history"][-2][1] - 1
    out["updated"] = _mtime(ds.partition_dir(parts[-1])) if parts else None
    if parts and expected and src.name in ("day_aggs", "tickers", "short_volume",
                                           "ibkr_borrow"):
        start = max(parts[0], expected - dt.timedelta(days=GAP_SESSIONS * 7 // 5))
        missing = dal.gaps(ds, start, expected)
        out["gaps"] = [_iso(d) for d in missing]
    return parts


def _current(src: Source, max_age: int, now: dt.datetime, out: dict) -> None:
    f = src.ds.table_file
    out["partitions"] = None
    if not f.exists():
        out.update(level="warn", reason="not pulled yet")
        return
    out["latest_rows"] = _row_counts([f]).get(str(f))
    out["updated"] = _mtime(f)
    when = dt.datetime.fromtimestamp(f.stat().st_mtime, dt.UTC)
    age = (now.date() - when.date()).days
    out["level"] = "ok" if age <= max_age else "warn"
    out["reason"] = (f"refreshed {age} day{'s' if age != 1 else ''} ago; limit {max_age}"
                     if max_age else f"refreshed {age} days ago; pulled each run")
    nxt = when + dt.timedelta(days=max_age) if max_age else None
    out["next"] = f"due {nxt:%Y-%m-%d}" if nxt and nxt > now else "the next run"


# ---------- extras: what each dataset says about itself ----------

def _extras(src: Source, parts: list[dt.date], now: dt.datetime) -> list[list[str]]:
    from sdp.ingest import etf_holdings

    ds, out = src.ds, []
    newest = ds.partition_file(parts[-1]) if parts and not ds.per_member else None
    if src.name == "tickers" and newest:
        r = _one(f"""select count(*) filter (where type = 'CS'), count(*) filter (where type =
                     'ETF'), count(*) filter (where composite_figi is null)
                     from read_parquet('{newest}')""")
        if r:
            out += [["common stock", f"{r[0]:,}"], ["ETFs", f"{r[1]:,}"],
                    ["no FIGI", f"{r[2]:,}"]]
    elif src.name == "day_aggs" and newest:
        r = _one(f"select sum(close * volume) from read_parquet('{newest}')")
        if r and r[0]:
            out.append(["dollar volume", f"${r[0] / 1e9:,.0f}bn"])
    elif src.name in ("splits", "dividends") and ds.table_file.exists():
        col = "execution_date" if src.name == "splits" else "ex_dividend_date"
        r = _one(f"""select count(*) filter (where {col} > current_date), max(vendor_pull_date),
                     min({col}), count(distinct ticker) from read_parquet('{ds.table_file}')""")
        pulls = len(list((settings.vendor_dir / ds.name).glob("*")))
        if r:
            out += [["announced, ahead", f"{r[0]:,}"], ["tickers", f"{r[3]:,}"],
                    ["since", _iso(r[2]) or "–"], ["vendor pulls kept", str(pulls)]]
    elif src.name == "fred_series" and ds.table_file.exists():
        r = _one(f"""with last as (select series_id, max(date) filter (where value is not null)
                     as d from read_parquet('{ds.table_file}') group by 1)
                     select count(*), count(*) filter (where d < current_date - 21), max(d)
                     from last""")
        if r:
            out += [["series", str(r[0])], ["stale over 21 days", str(r[1])],
                    ["newest observation", _iso(r[2]) or "–"]]
    elif src.name in ("french_factors", "q_factors") and ds.table_file.exists():
        r = _one(f"select min(date), max(date) from read_parquet('{ds.table_file}')")
        if r:
            out += [["data from", _iso(r[0]) or "–"], ["data to", _iso(r[1]) or "–"]]
    elif src.name == "short_interest" and parts:
        out.append(["last settlement", _iso(parts[-1])])
    elif src.name == "ticker_details" and parts:
        out.append(["last month-end", _iso(parts[-1])])
    elif src.name == "ibkr_borrow" and newest:
        r = _one(f"""select min(snapshot_at), count(*) filter (where available_capped),
                     count(*) filter (where fee_rate >= 10), median(fee_rate),
                     count(*) filter (where composite_figi is null)
                     from read_parquet('{newest}')""")
        if r:
            from sdp.ingest.common import EASTERN
            et = r[0].astimezone(EASTERN) if r[0] else None
            out += [["snapshot", f"{et:%H:%M} ET" if et else "–"],
                    ["fee 10% or more", f"{r[2]:,}"], ["median fee", f"{r[3]:.2f}%"],
                    ["at the 10M cap", f"{r[1]:,}"], ["no FIGI", f"{r[4]:,}"]]
    elif src.name == "etf_funds" and newest:
        r = duckdb.sql(f"""select issuer, count(*), sum(total_net_assets), max(nav_date)
                           from read_parquet('{newest}') group by 1 order by 1""").fetchall()
        for issuer, n, aum, nav in r:
            out.append([issuer, f"{n} funds, ${(aum or 0) / 1e12:.2f}tn, NAV {_iso(nav)}"])
    elif src.name == "etf_nav_history" and ds.table_file.exists():
        r = _one(f"""select count(distinct fund), min(date), max(date), max(vendor_pull_date)
                     from read_parquet('{ds.table_file}')""")
        if r:
            out += [["funds", str(r[0])], ["from", _iso(r[1]) or "–"],
                    ["to", _iso(r[2]) or "–"], ["pulled", _iso(r[3]) or "–"]]
    elif src.name == "etf_holdings":
        funds = etf_holdings.read_funds()
        newest_by_fund = etf_holdings.newest_dates()
        from sdp.ingest.common import eastern, previous_session
        floor = previous_session(previous_session(eastern(now).date()))
        lagging = sorted(f["fund"] for f in funds
                         if newest_by_fund.get(f["fund"], dt.date.min) < floor)
        classes: dict[str, int] = {}
        for f in funds:
            classes[f["asset_class"]] = classes.get(f["asset_class"], 0) + 1
        out += [["funds listed", str(len(funds))],
                ["by class", ", ".join(f"{k} {v}" for k, v in sorted(classes.items()))],
                ["current", f"{len(funds) - len(lagging)} of {len(funds)}"]]
        if lagging:
            out.append(["behind", ", ".join(lagging[:12]) + (" …" if len(lagging) > 12 else "")])
    return out


# ---------- the snapshot ----------

def _dataset(src: Source, now: dt.datetime, expected: dt.date | None,
             last_pull: dict | None) -> dict:
    from sdp import daily
    from sdp.ingest import etf_holdings, etf_nav_history, ibkr_borrow
    from sdp.ingest.common import eastern, is_session, previous_session

    out: dict = {"name": src.name, "group": src.group, "title": src.title,
                 "provider": src.provider, "cadence": src.cadence, "what": src.what,
                 "model": src.model, "unit": src.unit, "kind": "event",
                 "behind": None, "level": "ok", "reason": "", "next": None,
                 "first": None, "last": None, "partitions": None, "latest_rows": None,
                 "latest_change": None, "history": [], "gaps": [], "updated": None}
    out["raw_bytes"], _ = _size(src.ds.root)
    out["vendor_bytes"], out["vendor_files"] = _size(settings.vendor_dir / src.ds.name)
    et = eastern(now)
    parts: list[dt.date] = []

    if src.ds.key is None:
        out["kind"] = "current"
        max_age = {"splits": 0, "dividends": 0, "etf_nav_history":
                   etf_nav_history.MAX_AGE_DAYS}.get(src.name)
        if max_age is None:
            max_age = daily.PUBLIC[src.ds.name].MAX_AGE_DAYS
        _current(src, max_age, now, out)
    else:
        parts = _event(src, expected, out)
        last = parts[-1] if parts else None
        if src.name in ("day_aggs", "tickers", "short_volume"):
            behind = daily._sessions_behind(last, expected)
            out["behind"] = behind
            out["level"] = "bad" if behind is None else ("ok" if behind == 0 else "warn")
            out["reason"] = ("no partitions" if behind is None else "up to date" if behind == 0
                             else f"{behind} session{'s' if behind > 1 else ''} behind "
                                  f"{expected}")
            nxt = daily.backfill.sessions(last + dt.timedelta(days=1),
                                          last + dt.timedelta(days=10))[:1] if last else []
            if nxt:
                out["next"] = (f"{nxt[0]}, lands ~{nxt[0] + dt.timedelta(days=1)} 06:00 UTC"
                               if src.name == "day_aggs" else f"{nxt[0]}, on the day")
        elif src.name == "short_interest":
            checked = bool(last_pull and str(last_pull.get("time") or "")[:10]
                           == now.date().isoformat() and last_pull.get("exit_code") == 0
                           and not last_pull.get("offline"))
            out.update(kind="sparse", level="ok" if checked else "warn",
                       reason="checked by today's clean run" if checked
                       else "not checked by a clean run today",
                       next="the next settlement, about two weeks after the last")
        elif src.name == "ticker_details":
            from sdp.ingest import massive_ticker_details as details
            due = details.month_end_sessions(expected - dt.timedelta(days=40), expected) \
                if expected else []
            ok = bool(last and due and last >= due[-1])
            nxt = details.month_end_sessions(et.date(), et.date() + dt.timedelta(days=40))
            out.update(kind="monthly", level="ok" if ok else "warn",
                       reason="has the last month-end" if ok else "a month-end is missing",
                       next=f"{nxt[0]}" if nxt else None)
        elif src.name == "ibkr_borrow":
            due = et.date() if is_session(et.date()) and et.hour >= ibkr_borrow.PULL_HOUR_ET \
                else previous_session(et.date())
            behind = daily._sessions_behind(last, due)
            out["behind"] = behind
            out["level"] = "bad" if behind is None else ("ok" if behind == 0 else "warn")
            out["reason"] = ("no snapshot yet" if behind is None else "has the due snapshot"
                             if behind == 0 else f"{behind} session snapshot(s) missing; "
                                                 "they cannot be recovered")
            start = et.date() + dt.timedelta(days=1) if last == et.date() else et.date()
            nxt = daily.backfill.sessions(start, start + dt.timedelta(days=10))[:1]
            out["next"] = f"{nxt[0]} from 08:00 ET" if nxt else None
        elif src.name == "etf_funds":
            out["behind"] = (et.date() - last).days if last else None
            out["level"] = "ok" if last == et.date() else ("warn" if last else "bad")
            out["reason"] = ("has today's snapshot" if last == et.date()
                             else f"last snapshot {last}" if last else "no snapshot yet")
            out["next"] = f"{et.date() + dt.timedelta(days=1)}, the first run of the day"
        elif src.name == "etf_holdings":
            funds = etf_holdings.read_funds()
            newest = etf_holdings.newest_dates()
            floor = previous_session(previous_session(et.date()))
            current = sum(1 for f in funds if newest.get(f["fund"], dt.date.min) >= floor)
            out["level"] = "ok" if funds and current >= 0.9 * len(funds) else "warn"
            out["reason"] = (f"{current} of {len(funds)} funds within a session"
                             if funds else "no funds listed; run --select")
            out["next"] = "each run, per fund"
    out["extras"] = _extras(src, parts, now)
    return out


def _runs(n: int = 48) -> list[dict]:
    path = settings.data_root / "_logs" / "runs.jsonl"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()[-n:]
    except OSError:
        return []
    out = []
    for ln in lines:
        try:
            out.append(json.loads(ln))
        except ValueError:
            continue
    return out


def _pipeline(now: dt.datetime, last_pull: dict | None) -> dict:
    from sdp import daily

    local = now.astimezone()
    nxt = local.replace(minute=5, second=0, microsecond=0)
    if nxt <= local:
        nxt += dt.timedelta(hours=1)
    lock = daily._lock_dir()
    running = None
    if lock.exists() and not daily._lock_is_stale(lock):
        running = "an update holds the lock" + daily._lock_holder(lock)
    backfill = daily._backfill_lock_dir()
    if backfill.exists() and not daily._lock_is_stale(backfill):
        running = "a backfill holds the lock" + daily._lock_holder(backfill)
    return {"last_pull": last_pull, "runs": _runs(), "running": running,
            "next_run": nxt.astimezone(dt.UTC).isoformat(),
            "schedule": "launchd com.sdp.daily, every hour at :05"}


def _warehouse() -> dict:
    from sdp import daily, transform

    path = settings.warehouse_path
    stamp = transform.last_build()
    out: dict = {"exists": path.exists(), "bytes": path.stat().st_size if path.exists() else 0,
                 "built": stamp.get("published_utc") if stamp else None,
                 "argv": stamp.get("argv") if stamp else None,
                 "stale": daily.dbt_stale(), "schemas": [], "last_run": None,
                 "links_to_review": None}
    rr = Path(__file__).resolve().parents[2] / "transform" / "target" / "run_results.json"
    try:
        r = json.loads(rr.read_text(encoding="utf-8"))
        counts: dict[str, int] = {}
        for x in r["results"]:
            counts[x["status"]] = counts.get(x["status"], 0) + 1
        out["last_run"] = {
            "at": r["metadata"].get("generated_at"), "which": r["args"].get("which"),
            "select": r["args"].get("select") or [], "elapsed": r.get("elapsed_time"),
            "counts": counts,
            "issues": [{"node": x["unique_id"].split(".")[-1], "status": x["status"],
                        "message": x.get("message")}
                       for x in r["results"] if x["status"] not in ("success", "pass")][:20],
        }
    except (OSError, ValueError, KeyError):
        pass
    if path.exists():
        try:
            wh = dal.warehouse()
            try:
                out["schemas"] = [
                    {"schema": s, "tables": t, "views": v} for s, t, v in wh.execute("""
                        select table_schema, count(*) filter (where table_type = 'BASE TABLE'),
                               count(*) filter (where table_type = 'VIEW')
                        from information_schema.tables
                        where table_schema not in ('information_schema', 'pg_catalog', 'main')
                        group by 1 order by 1""").fetchall()]
                row = wh.execute("""select pending from monitor.market_sources
                                    where source = 'Security links'""").fetchone()
                out["links_to_review"] = row[0] if row else None
            finally:
                wh.close()
        except duckdb.Error:
            pass
    return out


def _storage() -> dict:
    root = settings.data_root
    parts = {name: _size(root / name)[0]
             for name in ("raw", "vendor", "warehouse", "_staging", "_logs")}
    return {"total": sum(parts.values()), **parts}


def snapshot(now_utc: dt.datetime | None = None, last_pull: dict | None = None) -> dict:
    """Return the status of every dataset, the pipeline, the warehouse and storage."""
    from sdp import daily

    now = now_utc or dt.datetime.now(dt.UTC)
    expected = daily._expected_last_session(now)
    last_pull = daily._read_last_pull() if last_pull is None else last_pull
    datasets = [_dataset(src, now, expected, last_pull) for src in CATALOG]
    wh = _warehouse()
    built = wh["built"]
    dbt = ({"built": built[:10], "stale": wh["stale"], "level": "bad" if wh["stale"] else "ok",
            "detail": f"built {built[:10]}"} if wh["exists"] and built
           else {"built": None, "stale": True, "level": "bad", "detail": "never built"})
    return {"generated": now.isoformat(), "expected_last_session": _iso(expected),
            "groups": GROUPS, "datasets": datasets, "dbt": dbt, "warehouse": wh,
            "pipeline": _pipeline(now, last_pull), "storage": _storage(),
            "last_pull": last_pull}

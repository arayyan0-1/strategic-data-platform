# src/sdp/market.py
"""The market monitor: one payload from the warehouse marts, and the page that shows it.

The status server (sdp.dashboard) serves the page at /market and the payload at
/market.json. The payload is cached until the warehouse stamp changes, so a new build
shows on the next poll. Each read opens a read-only connection and closes it, so the
server never holds the warehouse open.
"""
from __future__ import annotations

import datetime as dt
import decimal
import math
import threading
from pathlib import Path
from typing import Any

import duckdb

from sdp import dal

M = "main_marts"

_lock = threading.Lock()
_cache: dict[str, Any] = {"stamp": None, "payload": None}


def _clean(v: Any) -> Any:
    """Return a value that JSON can carry: a float for a DECIMAL, an ISO date, or None
    for NaN and infinity."""
    if isinstance(v, decimal.Decimal):
        v = float(v)
    if isinstance(v, float) and not math.isfinite(v):
        return None
    if isinstance(v, dt.date | dt.datetime):
        return v.isoformat()
    if isinstance(v, list):
        return [_clean(x) for x in v]
    return v


def _rows(wh: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    rel = wh.sql(sql)
    cols = rel.columns
    return [{c: _clean(v) for c, v in zip(cols, row, strict=True)} for row in rel.fetchall()]


def _section(errors: list[str], name: str, fn):
    """Run one section. A missing mart leaves the section empty and names it."""
    try:
        return fn()
    except duckdb.CatalogException:
        errors.append(f"{name}: a mart is missing. Run python -m sdp.transform build.")
        return None


def build(wh: duckdb.DuckDBPyConnection) -> dict:
    """Return the payload from an open warehouse connection."""
    errors: list[str] = []

    def exceptions():
        return _rows(wh, f"select * from {M}.mart_market_exceptions order by score desc limit 40")

    def board():
        rows = _rows(wh, f"select * from {M}.mart_market_board order by position")
        series = {r["ticker"]: r for r in _rows(wh, f"""
            select ticker, list(date order by date) as dates, list(px order by date) as px
            from {M}.mart_market_history
            where date > (select max(date) from {M}.mart_market_history) - interval 1 year
            group by ticker""")}
        for r in rows:
            h = series.get(r["ticker"], {})
            r["dates"], r["series"] = h.get("dates", []), h.get("px", [])
        return {"rows": rows}

    def relations():
        return _rows(wh, f"""
            select relation, any_value(label) as label, any_value(kind) as kind,
                   arg_max(value, date) as value, arg_max(change_21, date) as change_21,
                   arg_max(z_21, date) as z_21, arg_max(pctile, date) as pctile,
                   list(value order by date) as series
            from {M}.mart_market_relations
            group by relation
            order by relation""")

    def regime():
        return _rows(wh, f"select * from {M}.mart_market_regime order by metric")

    def breadth():
        return _rows(wh, f"""
            select * from {M}.mart_market_breadth
            where date > (select max(date) from {M}.mart_market_breadth) - interval 2 year
            order by date""")

    def factors():
        risk = _rows(wh, f"select * from {M}.mart_factor_risk order by family, factor")
        perf = _rows(wh, f"select * from {M}.mart_factor_performance order by family, factor")
        curves = _rows(wh, f"""
            with r as (
                select family, factor, date, ret
                from {M}.mart_factor_returns
                where ret is not null
                  and date > (select max(date) from {M}.mart_factor_returns) - interval 1 year
            )
            select family, factor,
                   list(date order by date) as dates,
                   list(level order by date) as levels
            from (select *, exp(sum(ln(1 + ret)) over (
                          partition by family, factor order by date
                          rows between unbounded preceding and current row)) - 1 as level
                  from r)
            group by family, factor""")
        corr = _rows(wh, f"select * from {M}.mart_factor_correlation")
        costs = _rows(wh, f"""
            select signal,
                   avg(gross) / nullif(stddev_samp(gross), 0) * sqrt(252) as sharpe_gross,
                   avg(net) / nullif(stddev_samp(net), 0) * sqrt(252)     as sharpe_net,
                   avg(gross) * 252                                       as gross_ann,
                   avg(cost) * 252                                        as cost_ann,
                   avg(turnover)                                          as turnover,
                   avg(cost) * 1e4                                        as cost_bps_day,
                   median(capacity) filter (where date > (select max(date)
                       from {M}.mart_signal_costs) - interval 1 year)     as capacity
            from {M}.mart_signal_costs
            group by signal
            order by sharpe_net desc""")
        return {"risk": risk, "performance": perf, "curves": curves, "correlation": corr,
                "costs": costs}

    def movers():
        out: dict[str, list] = {}
        for r in _rows(wh, f"select * from {M}.mart_market_movers order by list, rank"):
            out.setdefault(r["list"], []).append(r)
        return out

    def calendar():
        return _rows(wh, f"""
            select * from {M}.mart_market_calendar
            order by event_date, dollar_volume desc
            limit 80""")

    def short_interest():
        dtc = _rows(wh, f"""
            select * from {M}.mart_market_short_interest
            where days_to_cover is not null and short_value >= 1e7
            order by days_to_cover desc limit 10""")
        up = _rows(wh, f"""
            select * from {M}.mart_market_short_interest
            where change is not null and prev_short_interest >= 1e5 and short_value >= 1e7
            order by change desc limit 10""")
        head = _rows(wh, f"""
            select max(settlement_date) as settlement_date, max(effective_date) as effective_date
            from {M}.mart_market_short_interest""")
        return {"days_to_cover": dtc, "increase": up, **(head[0] if head else {})}

    payload = {
        "exceptions": _section(errors, "exceptions", exceptions),
        "board": _section(errors, "board", board),
        "relations": _section(errors, "relations", relations),
        "regime": _section(errors, "regime", regime),
        "breadth": _section(errors, "breadth", breadth),
        "factors": _section(errors, "factors", factors),
        "movers": _section(errors, "movers", movers),
        "calendar": _section(errors, "calendar", calendar),
        "short_interest": _section(errors, "short_interest", short_interest),
    }
    breadth_rows = payload["breadth"] or []
    payload["as_of"] = breadth_rows[-1]["date"] if breadth_rows else None
    payload["errors"] = errors
    return payload


def snapshot() -> dict:
    """Return the payload. Build it again only when the warehouse stamp changes."""
    from sdp import transform

    stamp = (transform.last_build() or {}).get("published_utc")
    with _lock:
        if _cache["payload"] is not None and _cache["stamp"] == stamp:
            return _cache["payload"]
        try:
            wh = dal.warehouse()
        except dal.MissingPartition as exc:
            return {"errors": [str(exc)], "built": None, "as_of": None}
        try:
            payload = build(wh)
        finally:
            wh.close()
        payload["built"] = stamp
        payload["generated"] = dt.datetime.now(dt.UTC).isoformat()
        _cache.update(stamp=stamp, payload=payload)
        return payload


# The page is a separate file, so an editor treats it as HTML.
PAGE = (Path(__file__).with_name("market.html")).read_text(encoding="utf-8")

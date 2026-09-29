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
        return _rows(wh, "select * from monitor.market_exceptions order by score desc limit 40")

    def board():
        rows = _rows(wh, "select * from monitor.market_board order by position")
        series = {r["ticker"]: r for r in _rows(wh, """
            select ticker, list(date order by date) as dates, list(adj_close order by date) as px
            from monitor.market_history
            where date > (select max(date) from monitor.market_history) - interval 1 year
            group by ticker""")}
        for r in rows:
            h = series.get(r["ticker"], {})
            r["dates"], r["series"] = h.get("dates", []), h.get("px", [])
        return {"rows": rows}

    def relations():
        return _rows(wh, """
            select relation, any_value(label) as label, any_value(kind) as kind,
                   arg_max(value, date) as value, arg_max(change_21, date) as change_21,
                   arg_max(z_21, date) as z_21, arg_max(pctile, date) as pctile,
                   list(value order by date) as series
            from monitor.market_relations
            group by relation
            order by relation""")

    def rates():
        # One date axis for every measure, and each series aligned to it with nulls.
        rows = _rows(wh, "select * from monitor.market_rates order by position")
        dates = [r["date"] for r in _rows(wh, """
            select distinct date from monitor.market_rates_history order by date""")]
        at = {d: i for i, d in enumerate(dates)}
        series: dict[str, list] = {}
        for r in _rows(wh, "select measure, date, value from monitor.market_rates_history"):
            series.setdefault(r["measure"], [None] * len(dates))[at[r["date"]]] = r["value"]
        for r in rows:
            r["series"] = series.get(r["measure"], [])
        return {"rows": rows, "dates": dates}

    def curve():
        pca = _rows(wh, "select * from monitor.market_curve_pca order by component, maturity")
        factors = _rows(wh, """
            select component, any_value(name) as name,
                   list(date order by date) as dates, list(value order by date) as values,
                   arg_max(move, date) as move, arg_max(z_1d, date) as z_1d
            from monitor.market_curve_factors
            group by component order by component""")
        return {"pca": pca, "factors": factors}

    def attribution():
        # The contributions summed over each span back from the last session.
        spans = _rows(wh, """
            with d as (
                select *, dense_rank() over (order by date desc) as back
                from monitor.market_attribution)
            select s.span, d.weighting, d.kind, d.component, sum(d.contribution) as contribution
            from d join (values ('1d', 1), ('1w', 5), ('1m', 21), ('3m', 63), ('1y', 252))
                s(span, n) on d.back <= s.n
            group by all""")
        names = _rows(wh, """
            select * from monitor.market_contributors order by span, side desc, rank""")
        return {"spans": spans, "contributors": names}

    def heatmap():
        return _rows(wh, "select * from monitor.market_heatmap order by cap desc")

    def signals():
        top = _rows(wh, """
            select s.signal, s.ticker, s.value, s.z, s.decile, s.rank_high, s.n_names,
                   u.name, u.industry_name, u.market_cap as cap
            from research.signal_snapshot s
            left join intermediate.int_universe u
                on u.security_key = s.security_key and u.date = s.date and u.is_primary_line
            where s.rank_high <= 10 or s.rank_high > s.n_names - 10
            order by s.signal, s.rank_high""")
        ic = _rows(wh, """
            select signal, target, horizon, mean_ic, t_nw, folds_same_sign
            from research.signal_ic_summary where period = 'development'""")
        return {"names": top, "ic": ic}

    def research_views():
        season = _rows(wh, "select * from research.market_seasonality")
        events = _rows(wh, "select * from research.event_study order by event, day")
        return {"seasonality": season, "events": events}

    def sources():
        return _rows(wh, "select * from monitor.market_sources")

    def labels():
        return {f"{r['family']}:{r['factor']}": r["label"]
                for r in _rows(wh, "select * from reference.factor_labels")}

    def regime():
        return _rows(wh, "select * from monitor.market_regime order by metric")

    def breadth():
        return _rows(wh, """
            select * from monitor.market_breadth
            where date > (select max(date) from monitor.market_breadth) - interval 2 year
            order by date""")

    def factors():
        risk = _rows(wh, "select * from factors.factor_risk order by family, factor")
        perf = _rows(wh, "select * from factors.factor_performance order by family, factor")
        curves = _rows(wh, """
            with r as (
                select family, factor, date, ret
                from factors.factor_returns
                where ret is not null
                  and date > (select max(date) from factors.factor_returns) - interval 1 year
            )
            select family, factor,
                   list(date order by date) as dates,
                   list(level order by date) as levels
            from (select *, exp(sum(ln(1 + ret)) over (
                          partition by family, factor order by date
                          rows between unbounded preceding and current row)) - 1 as level
                  from r)
            group by family, factor""")
        corr = _rows(wh, "select * from factors.factor_correlation")
        costs = _rows(wh, """
            select signal,
                   avg(gross) / nullif(stddev_samp(gross), 0) * sqrt(252) as sharpe_gross,
                   avg(net) / nullif(stddev_samp(net), 0) * sqrt(252)     as sharpe_net,
                   avg(gross) * 252                                       as gross_ann,
                   avg(cost) * 252                                        as cost_ann,
                   avg(turnover)                                          as turnover,
                   avg(cost) * 1e4                                        as cost_bps_day,
                   median(capacity) filter (where date > (select max(date)
                       from research.signal_costs) - interval 1 year)     as capacity
            from research.signal_costs
            group by signal
            order by sharpe_net desc""")
        return {"risk": risk, "performance": perf, "curves": curves, "correlation": corr,
                "costs": costs}

    def movers():
        out: dict[str, list] = {}
        for r in _rows(wh, "select * from monitor.market_movers order by list, rank"):
            out.setdefault(r["list"], []).append(r)
        return out

    def calendar():
        return _rows(wh, """
            select * from monitor.market_calendar
            order by event_date, dollar_volume desc
            limit 80""")

    def short_history():
        return _rows(wh, """
            select * from monitor.market_short_interest_history
            where settlement_date > (select max(settlement_date)
                                     from monitor.market_short_interest_history) - interval 3 year
            order by settlement_date""")

    def short_interest():
        dtc = _rows(wh, """
            select * from monitor.market_short_interest
            where days_to_cover is not null and short_value >= 1e7
            order by days_to_cover desc limit 10""")
        up = _rows(wh, """
            select * from monitor.market_short_interest
            where change is not null and prev_short_interest >= 1e5 and short_value >= 1e7
            order by change desc limit 10""")
        head = _rows(wh, """
            select max(settlement_date) as settlement_date, max(effective_date) as effective_date
            from monitor.market_short_interest""")
        return {"days_to_cover": dtc, "increase": up, **(head[0] if head else {})}

    payload = {
        "exceptions": _section(errors, "exceptions", exceptions),
        "board": _section(errors, "board", board),
        "relations": _section(errors, "relations", relations),
        "rates": _section(errors, "rates", rates),
        "curve": _section(errors, "curve", curve),
        "attribution": _section(errors, "attribution", attribution),
        "heatmap": _section(errors, "heatmap", heatmap),
        "signals": _section(errors, "signals", signals),
        "research": _section(errors, "research", research_views),
        "sources": _section(errors, "sources", sources),
        "labels": _section(errors, "labels", labels),
        "short_history": _section(errors, "short_history", short_history),
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

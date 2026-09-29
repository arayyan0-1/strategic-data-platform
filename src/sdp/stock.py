# src/sdp/stock.py
"""The views of the market monitor that read one security or a pair on request.

- search: the securities of the last session, for the command bar.
- profile: one stock through the style and industry model. Its exposures, its risk and
  the part of it that each factor explains, where its return came from, the names that
  move with it (in total, and in the stock-specific part, clipped at 3 sigma), its
  signals, short interest, corporate actions and largest stock-specific moves.
- pair: two tickers against each other: the ratio and its z-score, the rolling
  correlation and beta, the hedge ratio and the half-life of the ratio. The half-life
  shows only when a Dickey-Fuller test rejects a random walk at 5%.

Each view opens a read-only connection to the warehouse and closes it. A view is cached
until the build stamp changes.
"""
from __future__ import annotations

import math
import re
import threading
from collections import OrderedDict
from typing import Any

import duckdb
import numpy as np

from sdp import dal
from sdp.market import _clean
from sdp.risk import ew_cov

TICKER = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,11}$")
STYLES = ("size", "liquidity", "beta", "momentum", "reversal", "volatility",
          "dividend_yield", "high_52w")
INDUSTRIES = ("nodur", "durbl", "manuf", "enrgy", "chems", "buseq", "telcm", "utils",
              "shops", "hlth", "money", "other", "unknown")
WINDOWS = (("1d", 1), ("1w", 5), ("1m", 21), ("3m", 63), ("6m", 126), ("1y", 252))
# The covariance of the factor returns halves the weight of a session every
# FACTOR_HALF_LIFE sessions. The effective sample is then about 120 sessions, more than
# five times the 22 factors. The oldest of the FACTOR_WINDOW sessions has under 0.1% of the
# weight of the newest.
FACTOR_HALF_LIFE = 42
FACTOR_WINDOW = 504
CACHE_SIZE = 64
# The 5% critical value of the Dickey-Fuller t for a fit with a constant, 250 points.
ADF_CRITICAL = -2.87

_lock = threading.Lock()
_cache: OrderedDict[tuple, Any] = OrderedDict()


class UnknownTicker(LookupError):
    """No security has held the ticker in the lake."""


def _rows(wh: duckdb.DuckDBPyConnection, sql: str, params: list | tuple = ()) -> list[dict]:
    cur = wh.execute(sql, list(params))
    cols = [d[0] for d in cur.description]
    return [{c: _num(_clean(v)) for c, v in zip(cols, r, strict=True)} for r in cur.fetchall()]


def _num(v: Any) -> Any:
    """Return a value that JSON can carry for a NumPy scalar, None for NaN."""
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating | float):
        return float(v) if math.isfinite(v) else None
    return v


def _one(wh: duckdb.DuckDBPyConnection, sql: str, params: list | tuple = ()) -> dict | None:
    rows = _rows(wh, sql, params)
    return rows[0] if rows else None


def _ticker(t: str) -> str:
    t = (t or "").strip().upper()
    if not TICKER.match(t):
        raise UnknownTicker(f"{t!r} is not a ticker.")
    return t


def resolve(wh: duckdb.DuckDBPyConnection, ticker: str) -> str:
    """Return the security_key of the security that held the ticker last."""
    row = _one(wh, """
        select security_key from intermediate.int_universe
        where ticker = ?
        order by date desc, is_primary_line desc
        limit 1""", [_ticker(ticker)])
    if row is None:
        raise UnknownTicker(f"No security in the lake has held the ticker {ticker}.")
    return row["security_key"]


def search(wh: duckdb.DuckDBPyConnection) -> dict:
    """Return the securities of the last session, the most traded first, as compact rows
    of ticker, name, type, cap and trailing dollar volume."""
    rows = wh.execute("""
        select ticker, left(name, 48), type_filled, round(market_cap), round(adv)
        from intermediate.int_universe
        where date = (select max(date) from intermediate.int_universe) and is_primary_line
        order by coalesce(adv, dollar_volume, 0) desc""").fetchall()
    return {"rows": [[t, n, ty, _num(_clean(c)), _num(_clean(a))] for t, n, ty, c, a in rows]}


def _price_stats(px: np.ndarray, dates: list) -> dict:
    """Return the returns over the standard windows, the volatility, drawdown and the shape
    of the daily returns over the last year."""
    out: dict[str, Any] = {}
    for name, n in WINDOWS:
        out[f"ret_{name}"] = float(px[-1] / px[-1 - n] - 1) if len(px) > n else None
    this_year = [i for i, d in enumerate(dates) if d.year == dates[-1].year]
    if this_year and this_year[0] > 0:
        out["ret_ytd"] = float(px[-1] / px[this_year[0] - 1] - 1)
    r = np.diff(np.log(px[-253:]))
    if len(r) >= 20:
        m, s = r.mean(), r.std(ddof=1)
        out["vol_1y"] = float(s * math.sqrt(252))
        out["vol_1m"] = float(r[-21:].std(ddof=1) * math.sqrt(252))
        out["skew"] = float(((r - m) ** 3).mean() / s ** 3) if s > 0 else None
        out["kurtosis"] = float(((r - m) ** 4).mean() / s ** 4 - 3) if s > 0 else None
        out["share_up"] = float((r > 0).mean())
        year = px[-253:]
        out["max_drawdown_1y"] = float((year / np.maximum.accumulate(year) - 1).min())
        out["off_high_1y"] = float(px[-1] / year.max() - 1)
        out["over_low_1y"] = float(px[-1] / year.min() - 1)
    return out


def _risk(wh: duckdb.DuckDBPyConnection, expo: dict, spec_vol: float | None) -> dict | None:
    """Split the variance of the stock into the market, its industry, each style and the
    specific part (Euler: exposure times the covariance row). Annualized. The covariance
    of the factors weights recent sessions more (FACTOR_HALF_LIFE)."""
    names = ["market", *STYLES, *(f"ind_{i}" for i in INDUSTRIES)]
    F = np.array(wh.execute(f"""
        select {", ".join(f"coalesce({c}, 0)" for c in names)}
        from factors.style_factor_returns
        order by date desc limit {FACTOR_WINDOW}""").fetchall(), dtype=float)[::-1]
    if len(F) < 60 or spec_vol is None:
        return None
    x = np.zeros(len(names))
    x[0] = 1.0
    for j, s in enumerate(STYLES):
        x[1 + j] = expo.get(f"z_{s}") or 0.0
    ind = f"ind_{(expo.get('industry') or 'Unknown').lower()}"
    x[names.index(ind)] = 1.0
    omega = ew_cov(F, FACTOR_HALF_LIFE)
    parts = x * (omega @ x) * 252
    spec = spec_vol ** 2 * 252
    total = parts.sum() + spec
    out = {"market": parts[0], "industry": parts[names.index(ind)], "specific": spec}
    for j, s in enumerate(STYLES):
        out[s] = parts[1 + j]
    return {"vol": math.sqrt(total), "factor_vol": math.sqrt(max(parts.sum(), 0)),
            "spec_vol": math.sqrt(spec), "systematic_share": parts.sum() / total,
            "parts": [{"component": k, "var_share": v / total} for k, v in out.items()]}


def profile(wh: duckdb.DuckDBPyConnection, ticker: str) -> dict:
    """Return the profile of the stock that holds the ticker (see the module docstring)."""
    key = resolve(wh, ticker)
    head = _one(wh, """
        select s.security_key, s.ticker, s.name, s.type, s.key_rule, s.first_date, s.tickers,
               u.date as last_session, u.close, u.market_cap, u.industry, u.industry_name,
               u.sic_code, u.primary_exchange, u.adv, u.in_universe
        from core.securities s
        left join intermediate.int_universe u
            on u.security_key = s.security_key and u.is_primary_line
           and u.date = (select max(date) from intermediate.int_universe where security_key = ?)
        where s.security_key = ?""", [key, key])
    sic = _one(wh, """
        select sic_description, shares from intermediate.int_security_details
        where security_key = ? order by month_end desc limit 1""", [key]) or {}
    head.update(sic)
    last = head["last_session"]

    prices = wh.execute("""
        select date, coalesce(adj_close_total, adj_close_split) as px, volume, dollar_volume
        from core.security_sessions
        where security_key = ? and date > ?::date - interval 2 year
        order by date""", [key, last]).fetchall()
    prices = [p for p in prices if p[1] is not None and p[1] > 0]
    dates = [p[0] for p in prices]
    px = np.array([p[1] for p in prices], dtype=float)
    stats = _price_stats(px, dates) if len(px) > 1 else {}

    expo = _one(wh, f"""
        select date, industry, {", ".join(f"z_{s}" for s in STYLES)}
        from factors.style_exposures where security_key = ? order by date desc limit 1""", [key])
    expo_1y = peers_z = None
    if expo:
        expo_1y = _one(wh, f"""
            select date, {", ".join(f"z_{s}" for s in STYLES)}
            from factors.style_exposures
            where security_key = ? and date <= ?::date - interval 1 year
            order by date desc limit 1""", [key, expo["date"]])
        peers_z = _one(wh, f"""
            select count(*) as n_names, {", ".join(f"median(z_{s}) as z_{s}" for s in STYLES)}
            from factors.style_exposures where date = ? and industry = ?""",
                       [expo["date"], expo["industry"]])
    spec = _one(wh, """
        select spec_vol from factors.style_residuals
        where security_key = ? and spec_vol is not null order by date desc limit 1""", [key])
    risk = _risk(wh, expo, spec["spec_vol"] if spec else None) if expo else None

    parts = _rows(wh, f"""
        with d as (
            select date, lead(date) over (order by date) as next_date
            from (select distinct date from factors.style_exposures))
        select r.date, r.ret, r.market_part as market, coalesce(r.industry_part, 0) as industry,
               {", ".join(f"coalesce(e.z_{s} * f.{s}, 0) as {s}" for s in STYLES)},
               r.resid as specific, r.resid_z
        from factors.style_residuals r
        join d on d.next_date = r.date
        join factors.style_exposures e on e.security_key = r.security_key and e.date = d.date
        join factors.style_factor_returns f on f.date = r.date
        where r.security_key = ? and r.date > ?::date - interval 1 year
        order by r.date""", [key, last])
    components = ["market", "industry", *STYLES, "specific"]
    attribution = []
    for name, n in (("1m", 21), ("3m", 63), ("1y", 252)):
        span = parts[-n:]
        if span:
            attribution.append({"span": name, "ret": sum(p["ret"] for p in span),
                                **{c: sum(p[c] or 0 for p in span) for c in components}})
    cum_total = np.cumsum([p["ret"] for p in parts]).tolist()
    cum_spec = np.cumsum([p["specific"] or 0 for p in parts]).tolist()

    # A stock-specific move is clipped at 3 sigma, so one shared day of news cannot make
    # two names look related.
    related = _rows(wh, """
        with r as (
            select security_key, date, ret, greatest(least(resid_z, 3), -3) as z
            from factors.style_residuals
            where date > ?::date - interval 1 year and resid_z is not null),
        me as (select date, ret, z from r where security_key = ?),
        c as (
            select o.security_key, corr(o.z, me.z) as resid_corr,
                   corr(o.ret, me.ret) as ret_corr, count(*) as n_days
            from r o join me using (date)
            where o.security_key <> ?
            group by o.security_key
            having count(*) >= 200),
        top as (
            select *, 'specific' as kind from c order by resid_corr desc limit 10)
        select * from top
        union all
        (select *, 'total' from c order by ret_corr desc limit 10)""", [last, key, key])
    peers = []
    if head.get("industry") and head.get("market_cap"):
        peers = _rows(wh, """
            select security_key, abs(ln(market_cap / ?)) as distance
            from intermediate.int_universe
            where date = ? and is_primary_line and in_universe and industry = ?
              and security_key <> ? and market_cap > 0
            order by distance limit 10""", [head["market_cap"], last, head["industry"], key])
    keys = list({r["security_key"] for r in related + peers})
    info = {}
    if keys:
        info = {r["security_key"]: r for r in _rows(wh, """
            select u.security_key, u.ticker, u.name, u.industry_name, u.market_cap,
                   exp(sum(ln(1 + s.ret_1))) - 1 as ret_1m
            from intermediate.int_universe u
            left join research.signals s
                on s.security_key = u.security_key and s.date > ?::date - interval 30 day
               and s.date <= ?
            where u.date = ? and u.is_primary_line and list_contains(?, u.security_key)
            group by all""", [last, last, last, keys])}
    for r in related + peers:
        r.update({k: v for k, v in info.get(r["security_key"], {}).items() if k != "security_key"})

    signals = _rows(wh, """
        select p.signal, p.value, p.z, p.decile, p.rank_high, p.n_names, l.label,
               i.mean_ic, i.t_nw
        from research.signal_snapshot p
        left join reference.factor_labels l on l.family = 'quintile' and l.factor = p.signal
        left join research.signal_ic_summary i
            on i.signal = p.signal and i.target = 'residual' and i.horizon = 21
           and i.period = 'development'
        where p.security_key = ?
        order by p.signal""", [key])

    shorts = _rows(wh, """
        select s.settlement_date, s.effective_date, s.short_interest, s.days_to_cover,
               s.short_interest * u.close as short_value
        from intermediate.int_short_interest s
        left join intermediate.int_universe u on u.ticker = s.ticker and u.date = s.settlement_date
        where s.ticker = ?
        order by s.settlement_date desc limit 26""", [head["ticker"]])
    if sic.get("shares"):
        for s in shorts:
            s["share_of_shares"] = (s["short_interest"] or 0) / sic["shares"]

    actions = _rows(wh, """
        with t as (
            select ticker, min(date) as first_date, max(date) as last_date
            from intermediate.int_universe where security_key = ? group by ticker)
        select c.event_date, c.ticker, c.kind, c.ratio, c.cash_amount
        from intermediate.int_corporate_actions c
        join t on t.ticker = c.ticker
        where c.event_date >= greatest(t.first_date, ?::date - interval 3 year)
          and (c.event_date <= t.last_date or t.last_date = ?)
        order by c.event_date desc limit 40""", [key, last, last])
    shocks = _rows(wh, """
        select date, ret, resid, resid_z from factors.style_residuals
        where security_key = ? and abs(resid_z) >= 3
        order by date desc limit 20""", [key])
    cost = _one(wh, """
        select date, spread, spread_ar, sigma from research.spreads
        where security_key = ? order by date desc limit 1""", [key])

    return {
        "head": head, "stats": stats, "cost": cost,
        "prices": {"dates": [d.isoformat() for d in dates], "px": px.tolist()},
        "exposures": expo, "exposures_1y": expo_1y, "industry_median": peers_z,
        "risk": risk, "attribution": attribution,
        "cumulative": {"dates": [p["date"] for p in parts], "total": cum_total,
                       "specific": cum_spec},
        "related": related, "peers": peers, "signals": signals, "short_interest": shorts,
        "actions": actions, "shocks": shocks,
    }


def _rolling(fn, n: int, *xs: np.ndarray) -> list:
    """Return fn over each trailing window of n points, None before the first full one."""
    return [None if i + 1 < n else _num(fn(*(x[i + 1 - n:i + 1] for x in xs)))
            for i in range(len(xs[0]))]


def _corr(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.corrcoef(a, b)[0, 1])


def _beta(a: np.ndarray, b: np.ndarray) -> float:
    v = b.var(ddof=1)
    return float(np.cov(a, b)[0, 1] / v) if v > 0 else float("nan")


def reversion(x: np.ndarray) -> tuple[float | None, float | None]:
    """Return the Dickey-Fuller t of the series x and its half-life in sessions.

    The fit is dx(t) = a + b * x(t-1) + e by least squares. The half-life comes from the
    AR(1) coefficient 1 + b. It is None when t is not below ADF_CRITICAL, because the slope
    of a random walk is negative in most samples. A coefficient of 0 or less gives a
    half-life of 0.
    """
    dx, w = np.diff(x), x[:-1] - x[:-1].mean()
    sww = float(w @ w)
    if len(dx) < 3 or sww <= 0:
        return None, None
    slope = float(w @ dx) / sww
    e = dx - dx.mean() - slope * w
    s2 = float(e @ e) / (len(dx) - 2)
    if s2 <= 0:
        return None, None
    t = slope / math.sqrt(s2 / sww)
    if t >= ADF_CRITICAL:
        return t, None
    return t, -math.log(2) / math.log(1 + slope) if slope > -1 else 0.0


def pair(wh: duckdb.DuckDBPyConnection, a: str, b: str) -> dict:
    """Return the pair view of the tickers a and b (see the module docstring)."""
    ka, kb = resolve(wh, a), resolve(wh, b)
    rows = wh.execute("""
        with p as (
            select security_key, date, coalesce(adj_close_total, adj_close_split) as px
            from core.security_sessions
            where security_key in (?, ?) and date > current_date - interval 3 year)
        select a.date, a.px, b.px
        from p a join p b on b.date = a.date and b.security_key = ?
        where a.security_key = ? and a.px > 0 and b.px > 0
        order by a.date""", [ka, kb, kb, ka]).fetchall()
    rows = rows[-504:]
    if len(rows) < 30:
        raise UnknownTicker(f"{a} and {b} share fewer than 30 sessions.")
    dates = [r[0].isoformat() for r in rows]
    pa, pb = np.array([r[1] for r in rows]), np.array([r[2] for r in rows])
    x = np.log(pa / pb)
    ra, rb = np.diff(np.log(pa)), np.diff(np.log(pb))
    mean = _rolling(np.mean, 252, x)
    sd = _rolling(lambda v: v.std(ddof=1), 252, x)
    z = [None if m is None or not s else (x[i] - m) / s
         for i, (m, s) in enumerate(zip(mean, sd, strict=True))]
    corr = [None, *_rolling(_corr, 63, ra, rb)]
    beta = [None, *_rolling(_beta, 63, ra, rb)]
    adf_t, half_life = reversion(x[-252:])
    resid = _one(wh, """
        with r as (
            select security_key, date, resid from factors.style_residuals
            where security_key in (?, ?) and date > current_date - interval 1 year)
        select corr(a.resid, b.resid) as resid_corr, count(*) as n_days
        from r a join r b on b.date = a.date and b.security_key = ?
        where a.security_key = ?""", [ka, kb, kb, ka])
    names = {r["security_key"]: r for r in _rows(wh, """
        select security_key, ticker, name from core.securities where security_key in (?, ?)""",
                                                  [ka, kb])}
    rets = {}
    for name, n in WINDOWS[1:]:
        if len(pa) > n:
            rets[name] = [float(pa[-1] / pa[-1 - n] - 1), float(pb[-1] / pb[-1 - n] - 1)]
    return {
        "a": names.get(ka), "b": names.get(kb), "dates": dates,
        "ratio": (np.exp(x - x[0])).tolist(), "z": [_num(v) for v in z], "corr_63": corr,
        "beta_63": beta, "returns": rets,
        "stats": {"corr_1y": _num(_corr(ra[-252:], rb[-252:])),
                  "beta_1y": _num(_beta(ra[-252:], rb[-252:])),
                  "vol_a": float(ra[-252:].std(ddof=1) * math.sqrt(252)),
                  "vol_b": float(rb[-252:].std(ddof=1) * math.sqrt(252)),
                  "z_now": z[-1], "adf_t": _num(adf_t), "half_life": _num(half_life),
                  "resid_corr_1y": (resid or {}).get("resid_corr")},
    }


VIEWS = {"search": search, "profile": profile, "pair": pair}


def view(kind: str, *args: str) -> dict:
    """Return a view, from the cache when the build stamp is the same."""
    from sdp import transform

    stamp = (transform.last_build() or {}).get("published_utc")
    key = (stamp, kind, *args)
    with _lock:
        if key in _cache:
            _cache.move_to_end(key)
            return _cache[key]
    wh = dal.warehouse()
    try:
        out = VIEWS[kind](wh, *args)
    finally:
        wh.close()
    with _lock:
        _cache[key] = out
        while len(_cache) > CACHE_SIZE:
            _cache.popitem(last=False)
    return out

# src/sdp/stock.py
"""The views of the market monitor that read one security, a pair or a portfolio on request.

- search: the securities of the last session, for the command bar.
- profile: one security. Its route sets the model: the universe (the style and industry
  model), coverage (common stock and ADRs that the fit leaves out, on the same scale), a
  fund (loadings from a ridge fit, with macro factors) or none (price statistics only).
  The profile has its exposures, its risk and the part of it that each factor explains,
  where its return came from, the names that move with it, its signals, short interest,
  corporate actions and largest stock-specific moves.
- pair: two tickers against each other: the ratio and its z-score, the rolling
  correlation and beta, the hedge ratio and the half-life of the ratio. The half-life
  shows only when a Dickey-Fuller test rejects a random walk at 5%.
- portfolio: holdings with weights. Their exposures, risk and recent moves, split by
  factor and by holding. A request is never cached.

Each view opens a read-only connection to the warehouse and closes it. A view other than
the portfolio is cached until the build stamp changes.
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
# The factors of the style model, in the order of a loading vector.
MODEL = ("market", *STYLES, *(f"ind_{i}" for i in INDUSTRIES))
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

# The tables that hold the exposures and the residuals of each route.
EXPOSURES = {"universe": "factors.style_exposures", "coverage": "factors.coverage_exposures"}
RESIDUALS = {"universe": "factors.style_residuals", "coverage": "factors.coverage_residuals",
             "fund": "factors.fund_residuals"}
# The columns of fund_exposures that are not loadings. The other columns are macro factors.
FUND_FIXED = frozenset({"security_key", "ticker", "fit_date", "window_end", "last_date",
                        "r2_in", "n_obs", "ridge", "intercept"}) | frozenset(MODEL)
# The page shows a fund industry when its loading is this large, and the three largest always.
INDUSTRY_NOTE = 0.05
# The in-sample R2 of a fund fit above which the loadings read as the exposures of the fund,
# and above which they read as a partial picture.
R2_TIGHT = 0.9
R2_PARTIAL = 0.6
# For each macro factor: a label and the kind of the change. A yield and a spread change in
# percentage points. The others change in log points.
MACRO = {
    "d_t_10y": ("10-year Treasury yield", "yield"),
    "d_t_2y": ("2-year Treasury yield", "yield"),
    "d_real_10y": ("10-year real yield", "yield"),
    "d_hy_oas": ("High-yield credit spread", "spread"),
    "d_curve_10y_2y": ("Curve: 10-year less 2-year yield", "curve"),
    "d_dollar": ("Broad US dollar", "price"),
    "d_wti": ("WTI oil", "price"),
    "d_bitcoin": ("Bitcoin", "price"),
    "d_usdjpy": ("Yen per dollar (USDJPY)", "price"),
    "d_eurusd": ("Dollars per euro (EURUSD)", "price"),
}
COLUMN = re.compile(r"^[a-z0-9_]+$")

MAX_HOLDINGS = 60
PORTFOLIO_SESSIONS = 253
# A UK-listed fund is not in the lake. The closest US-listed fund is a suggestion.
UK_FUNDS = {"VWRL": "VT", "VWRP": "VT", "VWRA": "VT", "SWDA": "URTH", "IWDA": "URTH",
            "VUSA": "VOO", "VUAG": "VOO", "CSPX": "IVV", "EQQQ": "QQQ", "CNDX": "QQQ",
            "VFEM": "VWO", "EMIM": "IEMG"}

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
    t = (t or "").strip()
    if not TICKER.match(t.upper()):
        raise UnknownTicker(f"{t!r} is not a ticker.")
    return t


def resolve(wh: duckdb.DuckDBPyConnection, ticker: str) -> str:
    """Return the security_key of the security that held the ticker last. A ticker matches
    without regard to case, and the same case comes first."""
    t = _ticker(ticker)
    row = _one(wh, """
        select security_key from intermediate.int_universe
        where upper(ticker) = ?
        order by ticker = ? desc, date desc, is_primary_line desc
        limit 1""", [t.upper(), t])
    if row is None:
        raise UnknownTicker(f"No security in the lake has held the ticker {ticker}.")
    return row["security_key"]


def _tables(wh: duckdb.DuckDBPyConnection) -> set[str]:
    """Return the names of the tables in the schema factors, as schema.table."""
    return {f"{s}.{t}" for s, t in wh.execute("""
        select table_schema, table_name from information_schema.tables
        where table_schema = 'factors'""").fetchall()}


def _macro(wh: duckdb.DuckDBPyConnection, have: set[str]) -> list[str]:
    """Return the macro factors of the fund fits, in the order of the table."""
    if "factors.fund_exposures" not in have:
        return []
    cols = [r[0] for r in wh.execute("describe factors.fund_exposures").fetchall()]
    return [c for c in cols if c not in FUND_FIXED and COLUMN.match(c)]


def _route(wh: duckdb.DuckDBPyConnection, have: set[str], key: str, last: str | None) -> str:
    """Return the route of a security on its last session: universe when the style model
    has exposures for it, coverage when the coverage table has them, fund when a fit covers
    the session, else none."""
    if last is None:
        return "none"
    for route, table in EXPOSURES.items():
        if table in have and _one(
                wh, f"select 1 as x from {table} where security_key = ? and date = ?::date",
                [key, last]):
            return route
    if "factors.fund_exposures" in have and _one(wh, """
            select 1 as x from factors.fund_exposures
            where security_key = ? and ?::date between fit_date and last_date""", [key, last]):
        return "fund"
    return "none"


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


def _factor_cov(wh: duckdb.DuckDBPyConnection, macro: list[str]) -> np.ndarray | None:
    """Return the exponentially weighted covariance of the daily factor returns over the
    last FACTOR_WINDOW sessions, in the order of MODEL and then macro. A null style return
    counts as 0. With macro factors, only the sessions with every macro change count, so
    a pending change does not enter as 0. Return None with fewer than 60 sessions."""
    style = ", ".join(f"coalesce(s.{c}, 0)" for c in MODEL)
    if macro:
        cols = style + ", " + ", ".join(f"x.{c}" for c in macro)
        full = " and ".join(f"x.{c} is not null" for c in macro)
        sql = f"""select {cols} from factors.style_factor_returns s
                  join factors.macro_factor_returns x on x.date = s.date
                  where {full} order by s.date desc limit {FACTOR_WINDOW}"""
    else:
        sql = f"""select {style} from factors.style_factor_returns s
                  order by s.date desc limit {FACTOR_WINDOW}"""
    F = np.array(wh.execute(sql).fetchall(), dtype=float)[::-1]
    return ew_cov(F, FACTOR_HALF_LIFE) if len(F) >= 60 else None


def _vector(route: str, row: dict | None, macro: list[str]) -> np.ndarray:
    """Return the exposures of a security in the order of MODEL and then macro. A stock has
    a market exposure of 1, its style z-scores and a 1 on its industry. A fund has its
    loadings. A security with no model has zeros."""
    names = [*MODEL, *macro]
    x = np.zeros(len(names))
    if row is None:
        return x
    if route in EXPOSURES:
        x[0] = 1.0
        for j, s in enumerate(STYLES):
            x[1 + j] = row.get(f"z_{s}") or 0.0
        ind = f"ind_{(row.get('industry') or 'Unknown').lower()}"
        if ind in names:
            x[names.index(ind)] = 1.0
    elif route == "fund":
        for j, c in enumerate(names):
            x[j] = row.get(c) or 0.0
    return x


def _split(names: list[str], x: np.ndarray, omega: np.ndarray, spec_vol: float) -> dict | None:
    """Split the variance of a security or a portfolio into the market, the industries
    together, each style, each macro factor and the specific part (Euler: exposure times
    the covariance row). Annualized. The shares sum to 1."""
    parts = x * (omega @ x) * 252
    spec = spec_vol ** 2 * 252
    total = parts.sum() + spec
    if not total > 0:
        return None
    industries = [i for i, n in enumerate(names) if n.startswith("ind_")]
    out = {"market": parts[0], "industry": parts[industries].sum(), "specific": spec}
    for j, s in enumerate(STYLES):
        out[s] = parts[1 + j]
    for i in range(len(MODEL), len(names)):
        out[names[i]] = parts[i]
    return {"vol": math.sqrt(total), "factor_vol": math.sqrt(max(parts.sum(), 0)),
            "spec_vol": math.sqrt(spec), "systematic_share": parts.sum() / total,
            "parts": [{"component": k, "var_share": v / total} for k, v in out.items()]}


def _risk(wh: duckdb.DuckDBPyConnection, route: str, row: dict, macro: list[str],
          spec_vol: float | None) -> dict | None:
    """Return the risk split of one security (see _split). The covariance of the factors
    weights recent sessions more (FACTOR_HALF_LIFE). A stock uses the style factors alone.
    A fund uses the style and macro factors together."""
    omega = _factor_cov(wh, macro)
    if omega is None or spec_vol is None:
        return None
    return _split([*MODEL, *macro], _vector(route, row, macro), omega, spec_vol)


def _macro_meta(name: str) -> dict:
    label, kind = MACRO.get(name, (name, "price"))
    return {"label": label, "kind": kind}


def _fund_payload(fit: dict, earlier: dict | None, macro: list[str]) -> dict:
    """Return the fit of a fund for the page: the loadings beside those of an earlier fit,
    and the statistics of the fit. A fund industry is of note when its loading is
    INDUSTRY_NOTE or more, and the three largest are always of note."""
    e = earlier or {}

    def pair(col: str) -> dict:
        return {"loading": fit.get(col), "earlier": e.get(col)}

    big = sorted(INDUSTRIES, key=lambda i: -abs(fit.get(f"ind_{i}") or 0.0))[:3]
    return {
        "fit_date": fit["fit_date"], "window_end": fit["window_end"],
        "last_date": fit["last_date"], "r2_in": fit["r2_in"], "n_obs": fit["n_obs"],
        "ridge": fit["ridge"], "alpha_year": (fit["intercept"] or 0.0) * 252,
        "earlier_fit_date": e.get("fit_date"),
        "market": pair("market"),
        "styles": [{"factor": s, **pair(s)} for s in STYLES],
        "industries": [{"factor": i, **pair(f"ind_{i}"),
                        "note": i in big or abs(fit.get(f"ind_{i}") or 0.0) >= INDUSTRY_NOTE}
                       for i in INDUSTRIES],
        "macro": [{"factor": m, **_macro_meta(m), **pair(m)} for m in macro],
    }


def _route_note(route: str, head: dict, fund: dict | None) -> str:
    """Return one line that says what the route is and how far to trust it."""
    if route == "universe":
        return ("Common stock in the model universe. The fit of each session includes it, "
                "so this is the most reliable split.")
    if route == "coverage":
        gaps = []
        if not head.get("market_cap") or head["market_cap"] <= 0:
            gaps.append("The vendor gives no market cap, so the size and liquidity "
                        "exposures are 0.")
        if (head.get("industry") or "Unknown") == "Unknown":
            gaps.append("The vendor gives no industry code, so the industry is Unknown.")
        return ("Common stock or ADR outside the model universe. The fit does not use it. "
                "Its exposures use the scale of the universe, so read the split as an "
                "estimate. " + " ".join(gaps)).strip()
    if route == "fund":
        r2 = fund["r2_in"]
        if r2 is None:
            trust = "The fit has no R2."
        elif r2 >= R2_TIGHT:
            trust = "The factors explain most of its variance, so the loadings are its exposures."
        elif r2 >= R2_PARTIAL:
            trust = ("The factors explain part of its variance. The rest comes from sources "
                     "that they do not hold.")
        else:
            trust = ("The factors explain little of its variance. Do not read the loadings "
                     "as its exposures.")
        r2_text = "none" if r2 is None else f"{r2:.2f}"
        return (f"Fund. The loadings come from a ridge fit over the {fund['n_obs']} sessions "
                f"before {fund['fit_date']}. They apply out of sample until the next fit. "
                f"In-sample R2 is {r2_text}. {trust}")
    return ("No factor model covers this security (for example a preferred, a warrant, a "
            "right, a unit, or a fund with under a year of returns). The page shows price "
            "statistics only.")


def _parts(wh: duckdb.DuckDBPyConnection, route: str, keys: list[str], since: str,
           macro: list[str]) -> list[dict]:
    """Return one row per security and session of the last year: the return and its parts.
    A stock has market, industry, each style and specific. A fund adds alpha and each
    macro factor, with pend_<factor> true when that change is pending. A security with no
    model has its price return as the specific part. The parts of a stock are its exposures
    of the session before times the factor returns of the session."""
    ph = ", ".join("?" * len(keys))
    if route in EXPOSURES:
        ex, rs = EXPOSURES[route], RESIDUALS[route]
        return _rows(wh, f"""
            with d as (
                select date, lead(date) over (order by date) as next_date
                from (select distinct date from {ex}))
            select r.security_key, r.date, r.ret, r.market_part as market,
                   coalesce(r.industry_part, 0) as industry,
                   {", ".join(f"coalesce(e.z_{s} * f.{s}, 0) as {s}" for s in STYLES)},
                   r.resid as specific, r.resid_z
            from {rs} r
            join d on d.next_date = r.date
            join {ex} e on e.security_key = r.security_key and e.date = d.date
            join factors.style_factor_returns f on f.date = r.date
            where r.security_key in ({ph}) and r.date > ?::date - interval 1 year
            order by r.security_key, r.date""", [*keys, since])
    if route == "fund":
        macro_cols = "".join(
            f", coalesce(e.{m} * x.{m}, 0) as {m}"
            f", coalesce(list_contains(x.pending, '{m}'), false) as pend_{m}" for m in macro)
        return _rows(wh, f"""
            select r.security_key, r.date, r.ret, r.alpha, r.market_part as market,
                   r.industry_part as industry,
                   {", ".join(f"e.{s} * f.{s} as {s}" for s in STYLES)}{macro_cols},
                   r.resid as specific, r.resid_z
            from factors.fund_residuals r
            join factors.fund_exposures e
                on e.security_key = r.security_key and e.fit_date = r.fit_date
            join factors.style_factor_returns f on f.date = r.date
            join factors.macro_factor_returns x on x.date = r.date
            where r.security_key in ({ph}) and r.date > ?::date - interval 1 year
            order by r.security_key, r.date""", [*keys, since])
    return _rows(wh, f"""
        select security_key, date, ret, ret as specific from (
            select security_key, date,
                   px / lag(px) over (partition by security_key order by date) - 1 as ret
            from (select security_key, date, coalesce(adj_close_total, adj_close_split) as px
                  from core.security_sessions
                  where security_key in ({ph}) and date > ?::date - interval 13 month))
        where ret is not null and date > ?::date - interval 1 year
        order by security_key, date""", [*keys, since, since])


def _related(wh: duckdb.DuckDBPyConnection, have: set[str], route: str, key: str,
             last: str) -> list[dict]:
    """Return the names that move with a security over the last year. A stock gets ten by
    the correlation of its stock-specific moves (each clipped at 3 sigma, so one shared day
    of news cannot make two names look related) and ten by the correlation of returns. The
    names come from the universe and from coverage together. A fund gets ten funds and ten
    stocks by the correlation of returns."""
    tables = [t for t in (RESIDUALS["universe"], RESIDUALS["coverage"]) if t in have]
    if not tables or route == "none":
        return []
    stocks = " union all ".join(
        f"select security_key, date, ret, resid_z from {t}" for t in tables)
    if route == "fund":
        return _rows(wh, f"""
            with pool as (
                select security_key, date, ret, 'stock' as kind from ({stocks})
                where date > ?::date - interval 1 year and ret is not null
                union all
                select security_key, date, ret, 'fund' from factors.fund_residuals
                where date > ?::date - interval 1 year and ret is not null),
            me as (select date, ret from pool where security_key = ?),
            c as (
                select o.security_key, o.kind, corr(o.ret, me.ret) as ret_corr,
                       count(*) as n_days
                from pool o join me using (date)
                where o.security_key <> ?
                group by o.security_key, o.kind
                having count(*) >= 200 and isfinite(corr(o.ret, me.ret)))
            select * exclude (rk) from (
                select *, row_number() over (partition by kind order by ret_corr desc) as rk
                from c)
            where rk <= 10
            order by kind, ret_corr desc""", [last, last, key, key])
    return _rows(wh, f"""
        with r as (
            select security_key, date, ret, greatest(least(resid_z, 3), -3) as z
            from ({stocks})
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


def _similar_funds(wh: duckdb.DuckDBPyConnection, fit: dict, macro: list[str],
                   omega: np.ndarray | None) -> list[dict]:
    """Return the ten funds with the nearest loadings in the same fit. The measure is the
    cosine of the two loading vectors after each loading is scaled by the volatility of its
    factor. The scale puts a duration and a stock beta in the same unit: the volatility of
    the part."""
    if omega is None:
        return []
    names = [*MODEL, *macro]
    rows = wh.execute(f"""
        select security_key, ticker, r2_in, {", ".join(names)}
        from factors.fund_exposures where fit_date = ?::date""", [fit["fit_date"]]).fetchall()
    if len(rows) < 2:
        return []
    keys = [r[0] for r in rows]
    V = np.nan_to_num(np.array([r[3:] for r in rows], dtype=float))
    sd = np.sqrt(np.diag(omega) * 252)
    own = np.nan_to_num(np.array([fit.get(c) for c in names], dtype=float)) * sd
    S = V * sd
    denom = np.linalg.norm(S, axis=1) * np.linalg.norm(own)
    cos = np.divide(S @ own, denom, out=np.full(len(rows), -2.0), where=denom > 0)
    out = []
    for i in np.argsort(-cos):
        if keys[i] == fit["security_key"]:
            continue
        out.append({"security_key": keys[i], "ticker": rows[i][1], "cosine": float(cos[i]),
                    "r2_in": _num(rows[i][2]), "market": float(V[i, 0])})
        if len(out) == 10:
            break
    return out


def _attach(wh: duckdb.DuckDBPyConnection, last: str, *lists: list[dict]) -> None:
    """Add the name, industry, cap and one-month return to each row that has a
    security_key."""
    keys = list({r["security_key"] for rows in lists for r in rows})
    if not keys:
        return
    info = {r["security_key"]: r for r in _rows(wh, """
        select u.security_key, u.ticker, u.name, u.industry_name, u.market_cap,
               exp(sum(ln(1 + s.ret_1))) - 1 as ret_1m
        from intermediate.int_universe u
        left join research.signals s
            on s.security_key = u.security_key and s.date > ?::date - interval 30 day
           and s.date <= ?
        where u.date = ? and u.is_primary_line and list_contains(?, u.security_key)
        group by all""", [last, last, last, keys])}
    for rows in lists:
        for r in rows:
            r.update({k: v for k, v in info.get(r["security_key"], {}).items()
                      if k != "security_key"})


def profile(wh: duckdb.DuckDBPyConnection, ticker: str) -> dict:
    """Return the profile of the security that holds the ticker (see the module docstring)."""
    key = resolve(wh, ticker)
    have = _tables(wh)
    head = _one(wh, """
        select s.security_key, s.ticker, s.name, s.type, s.key_rule, s.first_date, s.tickers,
               u.date as last_session, u.close, u.market_cap, u.industry, u.industry_name,
               u.sic_code, u.primary_exchange, u.adv, u.in_universe, u.type_filled
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
    route = _route(wh, have, key, last)
    macro = _macro(wh, have) if route == "fund" else []

    prices = wh.execute("""
        select date, coalesce(adj_close_total, adj_close_split) as px, volume, dollar_volume
        from core.security_sessions
        where security_key = ? and date > ?::date - interval 2 year
        order by date""", [key, last]).fetchall()
    prices = [p for p in prices if p[1] is not None and p[1] > 0]
    dates = [p[0] for p in prices]
    px = np.array([p[1] for p in prices], dtype=float)
    stats = _price_stats(px, dates) if len(px) > 1 else {}

    expo = expo_1y = peers_z = fit = fund = coverage = None
    if route in EXPOSURES:
        table = EXPOSURES[route]
        expo = _one(wh, f"""
            select date, industry, {", ".join(f"z_{s}" for s in STYLES)}
            from {table} where security_key = ? order by date desc limit 1""", [key])
        expo_1y = _one(wh, f"""
            select date, {", ".join(f"z_{s}" for s in STYLES)}
            from {table}
            where security_key = ? and date <= ?::date - interval 1 year
            order by date desc limit 1""", [key, expo["date"]])
        peers_z = _one(wh, f"""
            select count(*) as n_names, {", ".join(f"median(z_{s}) as z_{s}" for s in STYLES)}
            from factors.style_exposures where date = ? and industry = ?""",
                       [expo["date"], expo["industry"]])
        if route == "coverage":
            coverage = {"type_filled": head.get("type_filled"),
                        "missing_cap": not head.get("market_cap") or head["market_cap"] <= 0,
                        "missing_industry": (expo["industry"] or "Unknown") == "Unknown"}
            if coverage["missing_industry"]:
                peers_z = None  # The universe names with no industry are no peer group.
    elif route == "fund":
        fit = _one(wh, """
            select * from factors.fund_exposures
            where security_key = ? and ?::date between fit_date and last_date
            order by fit_date desc limit 1""", [key, last])
        earlier = _one(wh, """
            select * from factors.fund_exposures
            where security_key = ? and fit_date <= ?::date - interval 1 year
            order by fit_date desc limit 1""", [key, fit["fit_date"]])
        fund = _fund_payload(fit, earlier, macro)

    spec = None
    if route in RESIDUALS:
        spec = _one(wh, f"""
            select spec_vol from {RESIDUALS[route]}
            where security_key = ? and spec_vol is not null order by date desc limit 1""", [key])
    risk = None
    if route != "none":
        risk = _risk(wh, route, expo or fit, macro, spec["spec_vol"] if spec else None)

    parts = _parts(wh, route, [key], last, macro) if route != "none" else []
    components = ["market", "industry", *STYLES, "specific"]
    if route == "fund":
        components = ["alpha", "market", "industry", *STYLES, *macro, "specific"]
    attribution = []
    for name, n in (("1m", 21), ("3m", 63), ("1y", 252)):
        span = parts[-n:]
        if span:
            entry = {"span": name, "ret": sum(p["ret"] for p in span),
                     **{c: sum(p[c] or 0 for p in span) for c in components}}
            if route == "fund":
                entry["sessions"] = len(span)
                entry["pending"] = {m: n_days for m in macro
                                    if (n_days := sum(1 for p in span if p[f"pend_{m}"]))}
            attribution.append(entry)
    cumulative = {"dates": [p["date"] for p in parts],
                  "total": np.cumsum([p["ret"] for p in parts]).tolist(),
                  "specific": np.cumsum([p["specific"] or 0 for p in parts]).tolist()}
    if route == "fund":
        cumulative["alpha"] = np.cumsum([p["alpha"] or 0 for p in parts]).tolist()

    related = _related(wh, have, route, key, last) if last else []
    peers = []
    if route in EXPOSURES and head.get("industry") and head.get("market_cap"):
        peers = _rows(wh, """
            select security_key, abs(ln(market_cap / ?)) as distance
            from intermediate.int_universe
            where date = ? and is_primary_line and in_universe and industry = ?
              and security_key <> ? and market_cap > 0
            order by distance limit 10""", [head["market_cap"], last, head["industry"], key])
    similar = _similar_funds(wh, fit, macro, _factor_cov(wh, macro)) if route == "fund" else []
    if last:
        _attach(wh, last, related, peers, similar)

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
    shocks = _rows(wh, f"""
        select date, ret, resid, resid_z from {RESIDUALS[route]}
        where security_key = ? and abs(resid_z) >= 3
        order by date desc limit 20""", [key]) if route in RESIDUALS else []
    cost = _one(wh, """
        select date, spread, spread_ar, sigma from research.spreads
        where security_key = ? order by date desc limit 1""", [key])

    return {
        "route": route, "route_note": _route_note(route, head, fund),
        "head": head, "stats": stats, "cost": cost, "coverage": coverage, "fund": fund,
        "prices": {"dates": [d.isoformat() for d in dates], "px": px.tolist()},
        "exposures": expo, "exposures_1y": expo_1y, "industry_median": peers_z,
        "risk": risk, "attribution": attribution, "cumulative": cumulative,
        "related": related, "peers": peers, "similar_funds": similar,
        "signals": signals, "short_interest": shorts, "actions": actions, "shocks": shocks,
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


class BadHoldings(ValueError):
    """The holdings of a portfolio request are malformed."""


def parse_holdings(raw: Any) -> list[dict]:
    """Return the lines of a portfolio request as dicts with a ticker, an amount and a
    weight. The request is a list of dicts with a ticker and an amount (a percent, a
    fraction or a money value). A ticker that repeats adds its amounts, and the ticker CASH
    is cash. The weight is the amount over the sum of the amounts, so it needs no unit."""
    if not isinstance(raw, list) or not raw:
        raise BadHoldings("The holdings must be a list of lines.")
    if len(raw) > MAX_HOLDINGS:
        raise BadHoldings(f"A portfolio holds at most {MAX_HOLDINGS} lines.")
    merged: dict[str, dict] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise BadHoldings("Each line must have a ticker and an amount.")
        ticker, amount = item.get("ticker"), item.get("amount")
        if not isinstance(ticker, str) or not ticker.strip() or len(ticker) > 32:
            raise BadHoldings("Each line must have a ticker of up to 32 characters.")
        if isinstance(amount, bool) or not isinstance(amount, int | float) \
                or not math.isfinite(amount):
            raise BadHoldings("Each amount must be a finite number.")
        line = merged.setdefault(ticker.strip().upper(), {"ticker": ticker.strip(), "amount": 0.0})
        line["amount"] += float(amount)
    total = sum(line["amount"] for line in merged.values())
    if not total > 0:
        raise BadHoldings("The amounts must have a positive sum.")
    return [{"ticker": "CASH" if k == "CASH" else v["ticker"], "amount": v["amount"],
             "weight": v["amount"] / total, "cash": k == "CASH", "total": total}
            for k, v in merged.items()]


def combine(weights: np.ndarray, X: np.ndarray) -> np.ndarray:
    """Return the exposures of a portfolio: the sum of weight times exposures of each holding.
    X has one row per holding."""
    return weights @ X


def variance_split(weights: np.ndarray, X: np.ndarray, omega: np.ndarray,
                   spec_var: np.ndarray) -> dict:
    """Split the annual variance of a portfolio x'Omega x + sum of w^2 s^2, with x the
    combined exposures and s^2 the daily specific variance of each holding. Return the part
    of each factor, the specific part of each holding and the contribution of each holding.
    The parts of the factors and the specific parts sum to the total. So do the
    contributions."""
    x = combine(weights, X)
    omega_x = omega @ x
    by_factor = x * omega_x * 252
    specific = weights ** 2 * spec_var * 252
    by_holding = weights * (X @ omega_x) * 252 + specific
    return {"x": x, "by_factor": by_factor, "specific": specific, "by_holding": by_holding,
            "total": float(by_factor.sum() + specific.sum())}


def _unresolved_reason(ticker: str) -> str:
    t = ticker.strip().upper()
    if not TICKER.match(t):
        return "This text is not a ticker."
    base = t.removesuffix(".L")
    if base in UK_FUNDS:
        return (f"{t} is a UK-listed fund, and the lake holds US-listed securities only. "
                f"Enter a US-listed equivalent, for example {UK_FUNDS[base]}.")
    if t.endswith(".L"):
        return ("A UK-listed security is not in the lake. The lake holds US-listed "
                "securities only. Enter a US-listed equivalent.")
    return ("No security in the lake has held this ticker. The lake holds US-listed "
            "securities only. For a UK-listed fund, enter a US-listed equivalent.")


def portfolio(wh: duckdb.DuckDBPyConnection, lines: list[dict]) -> dict:
    """Return the view of a portfolio (see the module docstring). The lines come from
    parse_holdings. A ticker that does not resolve is reported with a reason, and the
    numbers leave its weight out."""
    have = _tables(wh)
    macro = _macro(wh, have)
    names = [*MODEL, *macro]
    cal = [d.isoformat() for (d,) in wh.execute(f"""
        select date from factors.style_factor_returns
        order by date desc limit {PORTFOLIO_SESSIONS}""").fetchall()][::-1]
    as_of = cal[-1]

    valid = sorted({ln["ticker"].upper() for ln in lines
                    if not ln["cash"] and TICKER.match(ln["ticker"].upper())})
    found = {r["t"]: r["security_key"] for r in _rows(wh, """
        select upper(ticker) as t, security_key from intermediate.int_universe
        where list_contains(?, upper(ticker))
        qualify row_number() over (
            partition by upper(ticker) order by date desc, is_primary_line desc) = 1""",
                                                      [valid])} if valid else {}
    keys = sorted(set(found.values()))
    heads = {r["security_key"]: r for r in _rows(wh, """
        with last as (
            select security_key, max(date) as d from intermediate.int_universe
            where list_contains(?, security_key) group by 1)
        select s.security_key, s.ticker, s.name, s.type, l.d as last_session,
               u.type_filled, u.market_cap, u.industry
        from core.securities s
        join last l using (security_key)
        left join intermediate.int_universe u
            on u.security_key = s.security_key and u.date = l.d and u.is_primary_line""",
                                                [keys])} if keys else {}

    holdings, warnings = [], []
    modelled = []  # (index in holdings, exposure row, spec_vol or None)
    for ln in lines:
        h = {"input": ln["ticker"], "ticker": ln["ticker"].upper(), "amount": ln["amount"],
             "weight": ln["weight"], "security_key": None, "name": None, "type": None,
             "route": None, "status": "ok", "reason": None, "last_session": None,
             "vol": None, "spec_vol": None, "var_share": 0.0, "days": 0}
        if ln["cash"]:
            h.update(ticker="CASH", name="Cash", route="cash", status="cash")
        elif ln["ticker"].upper() not in found or found[ln["ticker"].upper()] not in heads:
            h.update(status="unresolved", reason=_unresolved_reason(ln["ticker"]))
        else:
            info = heads[found[ln["ticker"].upper()]]
            route = _route(wh, have, info["security_key"], info["last_session"])
            h.update(security_key=info["security_key"], ticker=info["ticker"],
                     name=info["name"], type=info["type_filled"] or info["type"],
                     route=route, last_session=info["last_session"])
            if info["last_session"] < as_of:
                warnings.append(f"{info['ticker']} last traded on {info['last_session']}.")
            row = spec = None
            if route in EXPOSURES:
                row = _one(wh, f"""
                    select date, industry, {", ".join(f"z_{s}" for s in STYLES)}
                    from {EXPOSURES[route]} where security_key = ?
                    order by date desc limit 1""", [info["security_key"]])
            elif route == "fund":
                row = _one(wh, """
                    select * from factors.fund_exposures
                    where security_key = ? and ?::date between fit_date and last_date
                    order by fit_date desc limit 1""",
                           [info["security_key"], info["last_session"]])
            if route in RESIDUALS:
                spec = _one(wh, f"""
                    select spec_vol from {RESIDUALS[route]}
                    where security_key = ? and spec_vol is not null
                    order by date desc limit 1""", [info["security_key"]])
            modelled.append((len(holdings), row, spec["spec_vol"] if spec else None))
        holdings.append(h)

    # The return series of each resolved holding over the last year.
    series: dict[str, dict[str, np.ndarray]] = {}
    pending: dict[str, dict[str, np.ndarray]] = {}
    pos = {d: i for i, d in enumerate(cal)}
    comps = ["ret", "alpha", "market", "industry", *STYLES, *macro, "specific"]
    seen: dict[str, np.ndarray] = {}
    for route in ("universe", "coverage", "fund", "none"):
        ks = sorted({h["security_key"] for h in holdings if h["route"] == route})
        for r in (_parts(wh, route, ks, as_of, macro) if ks else []):
            i = pos.get(r["date"])
            if i is None:
                continue
            k = r["security_key"]
            s = series.setdefault(k, {c: np.zeros(len(cal)) for c in comps})
            for c in comps:
                if c in r:
                    s[c][i] = r[c] or 0.0
            p = pending.setdefault(k, {m: np.zeros(len(cal), dtype=bool) for m in macro})
            for m in macro:
                if r.get(f"pend_{m}"):
                    p[m][i] = True
            seen.setdefault(k, np.zeros(len(cal), dtype=bool))[i] = True

    omega = _factor_cov(wh, macro)
    idx, X, w, sv = [], [], [], []
    for i, row, spec_vol in modelled:
        h = holdings[i]
        k = h["security_key"]
        h["days"] = int(seen[k].sum()) if k in seen else 0
        if h["route"] == "none":
            r = series[k]["ret"][seen[k]] if k in seen else np.array([])
            sd = float(r.std(ddof=1)) if len(r) > 20 else None
            h["status"] = "no_model"
            h["reason"] = ("No factor model covers this security. All of its risk counts as "
                           "specific.")
            if sd is None:
                warnings.append(f"{h['ticker']} has too little history for a volatility. "
                                "The risk counts it as 0.")
            spec_vol = sd
        elif spec_vol is None:
            warnings.append(f"{h['ticker']} has no forecast of its stock-specific volatility "
                            "yet. The risk counts that part as 0.")
        idx.append(i)
        X.append(_vector(h["route"], row, macro))
        w.append(h["weight"])
        sv.append((spec_vol or 0.0) ** 2)
        h["spec_vol"] = None if spec_vol is None else spec_vol * math.sqrt(252)
    w_cash = sum(h["weight"] for h in holdings if h["status"] == "cash")
    w_lost = sum(h["weight"] for h in holdings if h["status"] == "unresolved")
    if w_lost:
        warnings.append(f"{w_lost * 100:.1f}% of the portfolio did not resolve. "
                        "The numbers leave it out.")

    exposures = risk = None
    if idx:
        w_a, X_a, sv_a = np.array(w), np.array(X), np.array(sv)
        x = combine(w_a, X_a)
        exposures = {
            "market": float(x[0]),
            "styles": [{"factor": s, "x": float(x[1 + j])} for j, s in enumerate(STYLES)],
            "industries": [{"factor": c, "x": float(x[1 + len(STYLES) + j])}
                           for j, c in enumerate(INDUSTRIES)],
            "macro": [{"factor": m, **_macro_meta(m), "x": float(x[len(MODEL) + j])}
                      for j, m in enumerate(macro)]}
        if omega is not None:
            v = variance_split(w_a, X_a, omega, sv_a)
            risk = _split(names, x, omega, math.sqrt(float((w_a ** 2 * sv_a).sum())))
            for j, i in enumerate(idx):
                h = holdings[i]
                h["var_share"] = float(v["by_holding"][j] / v["total"]) if v["total"] > 0 else 0.0
                own = X_a[j] @ omega @ X_a[j] * 252 + sv_a[j] * 252
                h["vol"] = math.sqrt(own) if own > 0 else None
            if risk is not None:
                risk["holdings"] = [{"ticker": holdings[i]["ticker"],
                                     "variance": float(v["by_holding"][j]),
                                     "share": holdings[i]["var_share"]}
                                    for j, i in enumerate(idx)]
                risk["variance"] = v["total"]

    moves = []
    if cal:
        rf = {d.isoformat(): r for d, r in wh.execute(
            "select date, rf from core.rates where date >= ?::date", [cal[0]]).fetchall()}
        cash = np.array([float(rf.get(d) or 0.0) for d in cal])
        total = {c: np.zeros(len(cal)) for c in comps}
        held = [h for h in holdings if h["security_key"] in series]
        for h in held:
            for c in comps:
                total[c] += h["weight"] * series[h["security_key"]][c]
        total["cash"] = w_cash * cash
        total["ret"] = total["ret"] + total["cash"]
        year = sum(1 for d in cal if d[:4] == cal[-1][:4])
        for name, n in (("1d", 1), ("1w", 5), ("1m", 21), ("3m", 63), ("ytd", year),
                        ("1y", 252)):
            if n > len(cal):
                continue
            sl = slice(len(cal) - n, len(cal))
            contrib = [{"ticker": h["ticker"], "contribution": float(
                h["weight"] * series[h["security_key"]]["ret"][sl].sum())} for h in held]
            if w_cash:
                contrib.append({"ticker": "CASH", "contribution": float(w_cash * cash[sl].sum())})
            contrib.sort(key=lambda c: -abs(c["contribution"]))
            late = [m for m in macro if any(
                pending[h["security_key"]][m][sl].any() for h in held
                if h["route"] == "fund" and h["weight"])]
            moves.append({"span": name, "sessions": n,
                          "parts": {c: float(v[sl].sum()) for c, v in total.items()},
                          "contributors": contrib[:10], "pending": late})

    return {
        "as_of": as_of, "input_total": lines[0]["total"], "holdings": holdings,
        "covered_weight": float(sum(h["weight"] for h in holdings
                                    if h["status"] in ("ok", "no_model", "cash"))),
        "cash_weight": float(w_cash), "exposures": exposures, "risk": risk, "moves": moves,
        "warnings": warnings,
    }


def portfolio_view(holdings: Any) -> dict:
    """Return the portfolio view of a request body. It opens its own connection and never
    uses the cache, because each request has its own holdings. Raise BadHoldings for a
    malformed request."""
    lines = parse_holdings(holdings)
    wh = dal.warehouse()
    try:
        return portfolio(wh, lines)
    finally:
        wh.close()


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

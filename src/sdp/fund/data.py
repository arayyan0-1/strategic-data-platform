"""Read the fund's inputs from the warehouse into arrays with one row per session."""
from __future__ import annotations

import bisect
import datetime as dt
from dataclasses import dataclass

import duckdb
import numpy as np

from sdp.risk import ew_cov

BENCHMARK = "ACWI"
STYLES = ("size", "liquidity", "beta", "momentum", "reversal", "volatility",
          "dividend_yield", "high_52w")
INDUSTRIES = ("nodur", "durbl", "manuf", "enrgy", "chems", "buseq", "telcm", "utils",
              "shops", "hlth", "money", "other", "unknown")
MODEL = ("market", *STYLES, *(f"ind_{i}" for i in INDUSTRIES))
FACTOR_HALF_LIFE = 42
FACTOR_WINDOW = 504


@dataclass
class Panel:
    """Session-by-fund arrays for every candidate fund (columns) and session (rows).

    ret is the close-to-close total return, NaN without a bar. adj_open and adj_close are on one
    total-return scale. fee is the expense ratio a year. rf is the return of cash on the session.
    bench_ret is the benchmark's return, which need not be a candidate.
    """

    dates: list[dt.date]
    funds: list[str]
    keys: list[str]
    ret: np.ndarray
    adj_open: np.ndarray
    adj_close: np.ndarray
    eligible: np.ndarray
    half_spread: np.ndarray
    sigma: np.ndarray
    adv: np.ndarray
    fee: np.ndarray
    rf: np.ndarray
    benchmark: str
    bench_ret: np.ndarray
    holdout_start: dt.date
    role: list[str] | None = None

    def roles(self, cols) -> np.ndarray:
        """The role of each column: equity, bond, gold, commodity, bitcoin or cash."""
        if self.role is None:
            return np.array(["equity"] * len(cols))
        return np.array([self.role[c] for c in cols])

    def is_cash(self, cols) -> np.ndarray:
        return self.roles(cols) == "cash"

    def col(self, fund: str) -> int:
        return self.funds.index(fund)

    def row(self, d: dt.date) -> int:
        return self.dates.index(d)


def _pivot(rows: list[tuple], dates: list[dt.date], keys: list[str], j: int) -> np.ndarray:
    r = {d: i for i, d in enumerate(dates)}
    c = {k: i for i, k in enumerate(keys)}
    out = np.full((len(dates), len(keys)), np.nan)
    for row in rows:
        if row[0] in c and row[1] in r and row[j] is not None:
            out[r[row[1]], c[row[0]]] = row[j]
    return out


def _security(wh: duckdb.DuckDBPyConnection, ticker: str) -> str:
    row = wh.execute(
        "select security_key from core.securities where ticker = ? "
        "order by last_date desc limit 1", [ticker]).fetchone()
    if row is None:
        raise LookupError(f"{ticker} is not in core.securities.")
    return row[0]


def load(wh: duckdb.DuckDBPyConnection, benchmark: str = BENCHMARK) -> Panel:
    """Read fund.fund_universe, the cash return and the benchmark's returns."""
    cal = wh.execute("select date, coalesce(rf, 0) from core.rates order by date").fetchall()
    dates = [d for d, _ in cal]
    rf = np.array([x for _, x in cal], dtype=float)
    cols = {r[0] for r in wh.execute("describe fund.fund_candidates").fetchall()}
    role = "role" if "role" in cols else "'equity'"
    meta = wh.execute(f"""
        select security_key, fund, expense_ratio, {role} from fund.fund_candidates
        where is_candidate and security_key in (select security_key from fund.fund_universe)
        order by fund""").fetchall()
    keys = [m[0] for m in meta]
    rows = wh.execute("""
        select security_key, date, ret, adj_open, adj_close, eligible::double, half_spread,
               sigma, adv
        from fund.fund_universe""").fetchall()
    arrays = [_pivot(rows, dates, keys, j) for j in range(2, 9)]
    bkey = _security(wh, benchmark)
    bench = wh.execute("""
        select date, adj_close_total / lag(adj_close_total) over (order by date) - 1
        from core.security_sessions where security_key = ? order by date""", [bkey]).fetchall()
    bench_ret = _pivot([(bkey, d, x) for d, x in bench], dates, [bkey], 2)[:, 0]
    holdout = wh.execute(
        "select min(date) from research.eval_sessions where period = 'holdout'").fetchone()[0]
    return Panel(
        dates=dates, funds=[m[1] for m in meta], keys=keys,
        ret=arrays[0], adj_open=arrays[1], adj_close=arrays[2],
        eligible=np.nan_to_num(arrays[3]).astype(bool), half_spread=arrays[4],
        sigma=arrays[5], adv=arrays[6],
        fee=np.array([m[2] if m[2] is not None else np.nan for m in meta], dtype=float),
        rf=rf, benchmark=benchmark, bench_ret=bench_ret, holdout_start=holdout,
        role=[m[3] for m in meta],
    )


@dataclass
class FactorData:
    """The fund risk model: loadings per fit, daily factor returns and specific volatility."""

    names: list[str]
    fits: dict[str, list[tuple[dt.date, dt.date, np.ndarray]]]
    factor_dates: list[dt.date]
    factors: np.ndarray
    spec_vol: dict[tuple[str, dt.date], float]

    def cov(self, keys: list[str], d: dt.date, fallback_var: np.ndarray) -> np.ndarray:
        """Return B Omega B' + D on decision date d for keys, daily.

        B is each fund's loadings that apply on d, Omega the EW covariance of the factor returns
        to d, and D the squared specific volatility on d. A fund with no fit on d has no factor
        part and fallback_var as its specific variance.
        """
        k = len(self.names)
        B = np.zeros((len(keys), k))
        spec = np.array(fallback_var, dtype=float)
        for i, key in enumerate(keys):
            for start, end, vec in self.fits.get(key, []):
                if start <= d <= end:
                    B[i] = vec
                    sv = self.spec_vol.get((key, d))
                    if sv is not None and np.isfinite(sv):
                        spec[i] = sv * sv
                    break
        n = bisect.bisect_right(self.factor_dates, d)
        F = self.factors[max(0, n - FACTOR_WINDOW):n]
        F = F[~np.isnan(F).any(axis=1)]
        omega = ew_cov(F, FACTOR_HALF_LIFE)
        return B @ omega @ B.T + np.diag(spec)


def load_factors(wh: duckdb.DuckDBPyConnection, keys: list[str]) -> FactorData:
    """Read the fund loadings, factor returns and specific volatility for keys."""
    have = {r[0] for r in wh.execute("describe factors.fund_exposures").fetchall()}
    xcols = {r[0] for r in wh.execute("describe factors.macro_factor_returns").fetchall()}
    macro = [c for c in sorted(have) if c.startswith("d_") and c in xcols]
    names = [*MODEL, *macro]
    cols = ", ".join(f"coalesce({c}, 0)" for c in names)
    fits: dict[str, list] = {}
    for key, start, end, *vec in wh.execute(
            f"select security_key, fit_date, last_date, {cols} from factors.fund_exposures "
            "where security_key in (select unnest(?)) order by fit_date", [keys]).fetchall():
        fits.setdefault(key, []).append((start, end, np.array(vec, dtype=float)))
    style = ", ".join(f"coalesce(s.{c}, 0)" for c in MODEL)
    mac = ", ".join(f"x.{c}" for c in macro)
    sel = f"{style}, {mac}" if macro else style
    rows = wh.execute(f"""
        select s.date, {sel} from factors.style_factor_returns s
        left join factors.macro_factor_returns x on x.date = s.date order by s.date""").fetchall()
    spec = {(k, d): v for k, d, v in wh.execute(
        "select security_key, date, spec_vol from factors.fund_residuals "
        "where security_key in (select unnest(?))", [keys]).fetchall()}
    return FactorData(
        names=names, fits=fits, factor_dates=[r[0] for r in rows],
        factors=np.array([r[1:] for r in rows], dtype=float), spec_vol=spec)

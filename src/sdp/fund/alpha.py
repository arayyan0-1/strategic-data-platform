"""Return forecasts for the fund: Grinold's rule on cross-sectional scores, and a trend view on
cash.

For an equity fund i with score z_i (the mean of the cross-sectional z-scores of momentum and
value, standardized again), Grinold's rule gives a one-month forecast IC * (omega_i / sqrt(12)) *
z_i, with IC the monthly long-history information coefficient of the score and omega_i the fund's
active volatility a year. Rebalanced monthly, that is 12 times a year: shrink * IC * sqrt(12) *
omega_i * z_i. A cash fund's active return is the benchmark's excess return with the sign reversed,
so its forecast is -shrink times the mean excess return of the benchmark's long-history proxy
in the current trend state. A US-equity fund may also carry the forecast of the stocks it holds
(fund_holdings_alpha), added to its own.
"""
from __future__ import annotations

import calendar
import datetime as dt
from dataclasses import dataclass

import duckdb
import numpy as np

from sdp.fund.data import Panel
from sdp.fund.objectives import SESSIONS_A_YEAR

SHRINK = 0.5
Z_CLIP = 3.0
TREND_PROXY = "region:developed"


@dataclass
class Forecast:
    """Signals per (fund, month end) as (mom, val, trend); the IC of the score; the proxy's mean
    excess return a year after a positive and after a negative trend."""

    signals: dict[tuple[str, dt.date], tuple]
    ic: float
    excess_up: float
    excess_down: float
    shrink: float = SHRINK
    cross: bool = True
    trend: bool = True
    hold: dict[tuple[str, dt.date], float] | None = None
    signal: str = "combo"
    trend_rule: float = 0.0


def month_end(d: dt.date) -> dt.date:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def zscore(x: np.ndarray) -> np.ndarray:
    """Cross-sectional z-score of the finite values, clipped at Z_CLIP; a missing value is 0."""
    out = np.zeros(len(x))
    ok = np.isfinite(x)
    if ok.sum() >= 3 and np.std(x[ok]) > 0:
        out[ok] = np.clip((x[ok] - x[ok].mean()) / x[ok].std(), -Z_CLIP, Z_CLIP)
    return out


def score(mom: np.ndarray, val: np.ndarray, signal: str = "combo") -> np.ndarray:
    """The score: momentum alone, or the mean of the two z-scores standardized again."""
    if signal == "mom":
        return zscore(mom)
    return zscore(zscore(mom) + zscore(val))


def source(f: Forecast):
    """Return an alpha source for backtest.run."""

    def alpha(p: Panel, t: int, cols: list[int], S_active: np.ndarray) -> np.ndarray:
        month = month_end(p.dates[t])
        cash = p.is_cash(cols)
        sig = np.array([f.signals.get((p.funds[c], month), (None, None, None)) for c in cols],
                       dtype=float).reshape(len(cols), 3)
        a = np.zeros(len(cols))
        eq = ~cash
        if f.cross and eq.sum() >= 3:
            omega = np.sqrt(np.clip(np.diag(S_active), 0.0, None) * SESSIONS_A_YEAR)
            a[eq] = (f.shrink * f.ic * np.sqrt(12) * omega[eq]
                     * score(sig[eq, 0], sig[eq, 1], f.signal))
        if f.hold:
            a[eq] += np.array([f.hold.get((p.funds[c], month), 0.0) or 0.0
                               for c in np.asarray(cols)[eq]])
        bench = f.signals.get((p.benchmark, month), (None, None, None))[2]
        if f.trend and cash.any() and bench is not None and np.isfinite(bench):
            a[cash] = -f.shrink * (f.excess_up if bench > 0 else f.excess_down)
        return a

    def cash_floor(p: Panel, t: int) -> float:
        """The trend rule: trend_rule in cash while the benchmark's trend is negative."""
        bench = f.signals.get((p.benchmark, month_end(p.dates[t])), (None, None, None))[2]
        if f.trend_rule > 0 and bench is not None and np.isfinite(bench) and bench < 0:
            return f.trend_rule
        return 0.0

    alpha.cash_floor = cash_floor
    return alpha


def load_hold(wh: duckdb.DuckDBPyConnection) -> dict[tuple[str, dt.date], float]:
    """Read the holdings forecast per (fund, month end)."""
    return {(f, m): a for f, m, a in wh.execute(
        "select fund, month, alpha_hold from fund.fund_holdings_alpha").fetchall()}


def load(wh: duckdb.DuckDBPyConnection, dev_end: dt.date, signal: str = "combo",
         **kw) -> Forecast:
    """Read fund_signals, the development IC of the score (the mean over the three
    long-history universes) and the trend proxy's excess returns by trend state."""
    signals = {(f, m): (a, b, c) for f, m, a, b, c in wh.execute(
        "select fund, month, mom, val, trend from fund.fund_signals").fetchall()}
    ic = wh.execute("""
        select avg(mean_ic) from fund.fund_signal_ic
        where signal = ? and period = 'development'""", [signal]).fetchone()[0]
    up, down = wh.execute("""
        select avg(fwd_excess) filter (where trend > 0) * 12,
               avg(fwd_excess) filter (where trend <= 0) * 12
        from fund.fund_signal_history
        where index_key = ? and month <= ? and trend is not null""",
        [TREND_PROXY, dev_end]).fetchone()
    return Forecast(signals=signals, ic=float(ic or 0.0), excess_up=float(up or 0.0),
                    excess_down=float(down or 0.0), signal=signal, **kw)

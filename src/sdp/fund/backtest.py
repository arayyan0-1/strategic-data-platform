"""Walk the fund forward: decide after the last close of each month, trade at the next open.

Units are on the total-return scale, so distributions are reinvested. Cash earns the rate of
core.rates. A trade costs the modelled half spread plus square-root impact. The holding rule of
the book bounds every target from below.
"""
from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass, field

import numpy as np

from sdp.fund import covariance
from sdp.fund.book import Book
from sdp.fund.data import FactorData, Panel
from sdp.fund.objectives import (
    ACTIVE_VAR_FLOOR,
    SESSIONS_A_YEAR,
    STRATEGIES,
    Problem,
    project,
)
from sdp.fund.optimizer import Budget

IMPACT = 0.60
# The trade size, as a share of NAV, at which a fund's cost per unit traded is quoted to the
# optimizer.
QUOTE_SIZE = 0.10
# A holding counts as a name in the summary from this weight.
MIN_NAME_WEIGHT = 0.001


@dataclass
class Config:
    strategy: str
    covariance: str = "qis"
    capital: float = 100_000.0
    cap: float = 1.0
    rule: str = "last_buy"
    lock_days: int = 30
    min_trade: float = 0.005
    window: int = 252
    risk_aversion: float = 100.0
    amortize: int = 63
    dedup_corr: float = 0.995
    budget: Budget = field(default_factory=Budget)


# An alpha source maps (panel, decision row, columns, active covariance) to a forecast a year per
# column, before fees. None means no forecast.
AlphaSource = Callable[[Panel, int, list[int], np.ndarray], np.ndarray]


def holds_cash(cfg: Config) -> bool:
    """Only the tracking-error-budget objectives may hold the cash funds."""
    return STRATEGIES[cfg.strategy][0] == "budget"


@dataclass
class Result:
    config: Config
    dates: list[dt.date]
    nav: np.ndarray
    bench: np.ndarray
    rf: np.ndarray
    rebalances: list[dict] = field(default_factory=list)


def decision_rows(p: Panel, start: dt.date, end: dt.date) -> list[int]:
    """Rows of the last session of each month from start, whose next session is on or
    before end."""
    return [i for i in range(len(p.dates) - 1)
            if p.dates[i] >= start and p.dates[i + 1] <= end
            and p.dates[i + 1].month != p.dates[i].month]


def trade_cost(value: float, half_spread: float, sigma: float, adv: float) -> float:
    """Return the cost of a trade of `value` dollars: half spread plus square-root impact."""
    v = abs(value)
    if v == 0:
        return 0.0
    hs = half_spread if np.isfinite(half_spread) else 0.0
    impact = IMPACT * sigma * np.sqrt(v / adv) if np.isfinite(sigma) and adv > 0 else 0.0
    return v * (hs + impact)


def universe(p: Panel, t: int, held: set[int], cfg: Config) -> list[int]:
    """Columns the fund may hold after row t: the eligible funds, one per group whose returns
    correlate at dedup_corr or more (the cheapest, then the most traded), plus every fund held
    and the benchmark fund."""
    cols = np.flatnonzero(p.eligible[t])
    cash = p.is_cash(cols)
    if not holds_cash(cfg):
        cols = cols[~cash]
    X = np.nan_to_num(p.ret[t - cfg.window + 1:t + 1][:, cols])
    vol = X.std(axis=0)
    cols, X = cols[vol > 0], X[:, vol > 0]
    order = np.lexsort((-np.nan_to_num(p.adv[t, cols]), np.nan_to_num(p.fee[cols], nan=1.0)))
    C = np.corrcoef(X, rowvar=False)
    kept: list[int] = []
    for j in order:
        if all(C[j, k] < cfg.dedup_corr for k in kept):
            kept.append(j)
    out = {int(cols[j]) for j in kept} | held
    if p.benchmark in p.funds:
        out.add(p.col(p.benchmark))
    return sorted(out)


def problem(p: Panel, t: int, cols: list[int], w0: np.ndarray, lower: np.ndarray,
            upper: np.ndarray, nav: float, cfg: Config, fd: FactorData | None,
            alpha: AlphaSource | None = None) -> Problem:
    rows = slice(t - cfg.window + 1, t + 1)
    X = np.nan_to_num(p.ret[rows][:, cols])
    y = np.nan_to_num(p.bench_ret[rows])
    n = len(cols)
    if p.benchmark in p.funds and p.col(p.benchmark) in cols:
        b, Xall, keys = cols.index(p.col(p.benchmark)), X, [p.keys[c] for c in cols]
    else:
        b, Xall, keys = n, np.column_stack([X, y]), [p.keys[c] for c in cols] + [None]
    if cfg.covariance == "factor":
        if fd is None:
            raise ValueError("The factor covariance needs FactorData.")
        Sall = fd.cov(keys, p.dates[t], Xall.var(axis=0, ddof=1))
    else:
        Sall = covariance.estimate(Xall, cfg.covariance)
    size = QUOTE_SIZE * nav
    cost = np.array([trade_cost(size, p.half_spread[t, c], p.sigma[t, c], p.adv[t, c]) / size
                     for c in cols])
    S_active = covariance.active(Sall, b)[:n, :n] + ACTIVE_VAR_FLOOR * np.eye(n)
    return Problem(
        names=[p.funds[c] for c in cols], X=X, y=y, S=Sall[:n, :n], S_active=S_active,
        fee=np.nan_to_num(p.fee[cols]),
        alpha=np.zeros(n) if alpha is None
        else np.asarray(alpha(p, t, cols, S_active), dtype=float),
        cost=cost, w0=w0, lower=lower, upper=upper,
        benchmark=p.benchmark, risk_aversion=cfg.risk_aversion, amortize=cfg.amortize,
        cash=p.is_cash(cols), budget=cfg.budget)


def _decide(p: Panel, book: Book, t: int, px: np.ndarray, cfg: Config,
            fd: FactorData | None, alpha: AlphaSource | None) -> tuple[dict[int, float], dict]:
    nav = book.value({f: px[p.col(f)] for f in book.funds()})
    held = {p.col(f) for f in book.funds()}
    cols = universe(p, t, held, cfg)
    trade_day = p.dates[t + 1]
    units = np.array([book.units(p.funds[c]) for c in cols])
    prices = px[cols]
    w0 = units * prices / nav
    locked = np.array([book.units(p.funds[c]) - book.sellable(p.funds[c], trade_day)
                       for c in cols])
    lower = np.maximum(locked * prices / nav, 0.0)
    upper = np.where(p.eligible[t, cols], cfg.cap, np.minimum(cfg.cap, w0))
    upper = np.maximum(upper, lower)
    prob = problem(p, t, cols, w0, lower, upper, nav, cfg, fd, alpha)
    failed = False
    try:
        raw = np.asarray(STRATEGIES[cfg.strategy][1](prob), dtype=float)
    except Exception:  # noqa: BLE001 - a failed solve keeps the current weights
        raw, failed = w0.copy(), True
    w = project(raw, lower, upper)
    info = {
        "date": p.dates[t], "n_names": len(cols), "n_locked": int((lower > 1e-9).sum()),
        "projection": float(np.abs(w - raw).sum()), "failed": failed,
        "te_ex_ante": float(np.sqrt(max(w @ prob.S_active @ w, 0.0) * SESSIONS_A_YEAR)),
        "vol_ex_ante": float(np.sqrt(max(w @ prob.S @ w, 0.0) * SESSIONS_A_YEAR)),
        "fee": float(w @ prob.fee),
        "cash": float(w[prob.cash].sum()),
        "alpha": float(w @ prob.alpha),
    }
    return dict(zip(cols, w, strict=True)), info


def _execute(p: Panel, book: Book, t: int, target: dict[int, float], px_open: np.ndarray,
             cfg: Config) -> dict:
    """Trade toward the target at the open of row t. Sales first, then buys within the cash."""
    day, quote = p.dates[t], t - 1
    funds = set(target) | {p.col(f) for f in book.funds()}
    nav = book.value({f: px_open[p.col(f)] for f in book.funds()})
    delta = {c: target.get(c, 0.0) * nav - book.units(p.funds[c]) * px_open[c] for c in funds}
    traded = cost_paid = 0.0
    for c, dv in sorted(delta.items(), key=lambda kv: kv[1]):
        f, price = p.funds[c], px_open[c]
        held = book.units(f) * price
        exit_all = target.get(c, 0.0) == 0.0 and held > 0
        if dv >= 0 or (abs(dv) < cfg.min_trade * nav and not exit_all):
            continue
        units = min(-dv / price, book.sellable(f, day))
        if units <= 0:
            continue
        cost = trade_cost(units * price, p.half_spread[quote, c], p.sigma[quote, c],
                          p.adv[quote, c])
        book.sell(f, units, day, price, cost)
        traded += units * price
        cost_paid += cost
    buys = {c: dv for c, dv in delta.items() if dv >= cfg.min_trade * nav}
    want = sum(buys.values())
    scale = min(1.0, max(book.cash, 0.0) / want / 1.001) if want > 0 else 0.0
    for c, dv in buys.items():
        value = dv * scale
        if value <= 0:
            continue
        cost = trade_cost(value, p.half_spread[quote, c], p.sigma[quote, c], p.adv[quote, c])
        book.buy(p.funds[c], value / px_open[c], day, px_open[c], cost)
        traded += value
        cost_paid += cost
    return {"turnover": traded / nav / 2, "cost": cost_paid / nav}


def run(p: Panel, cfg: Config, start: dt.date, end: dt.date,
        fd: FactorData | None = None, alpha: AlphaSource | None = None) -> Result:
    """Run one strategy from the first decision date on or after start to end."""
    if cfg.strategy not in STRATEGIES:
        raise ValueError(f"Unknown strategy {cfg.strategy!r}. Known: {sorted(STRATEGIES)}.")
    rows = decision_rows(p, start, end)
    if not rows:
        raise ValueError(f"No decision date between {start} and {end}.")
    if rows[0] < cfg.window:
        raise ValueError(f"The first decision date {p.dates[rows[0]]} has less than "
                         f"{cfg.window} sessions before it.")
    last = max(i for i, d in enumerate(p.dates) if d <= end)
    book = Book(cash=cfg.capital, rule=cfg.rule, lock_days=cfg.lock_days)
    px = np.full(len(p.funds), np.nan)
    decisions = set(rows)
    nav, bench, rf = [], [], []
    pending: tuple[dict[int, float], dict] | None = None
    result = Result(config=cfg, dates=p.dates[rows[0]:last + 1], nav=np.empty(0),
                    bench=np.empty(0), rf=np.empty(0))
    for t in range(rows[0], last + 1):
        if t > rows[0]:
            book.cash *= 1 + p.rf[t]
        if pending is not None:
            px_open = np.where(np.isfinite(p.adj_open[t]), p.adj_open[t], px)
            info = pending[1] | _execute(p, book, t, pending[0], px_open, cfg)
            info["trade_date"] = p.dates[t]
            info["weights"] = {p.funds[c]: w for c, w in pending[0].items() if w > 1e-6}
            result.rebalances.append(info)
            pending = None
        px = np.where(np.isfinite(p.adj_close[t]), p.adj_close[t], px)
        nav.append(book.value({f: px[p.col(f)] for f in book.funds()}))
        bench.append(np.nan_to_num(p.bench_ret[t]) if t > rows[0] else 0.0)
        rf.append(p.rf[t] if t > rows[0] else 0.0)
        if t in decisions and t < last:
            pending = _decide(p, book, t, px, cfg, fd, alpha)
    result.nav = np.array(nav)
    result.bench = cfg.capital * np.cumprod(1 + np.array(bench))
    result.rf = np.array(rf)
    return result


def summary(r: Result) -> dict:
    """Return the headline numbers of a run, a year where it applies."""
    # The first session is the decision close and the entry; returns count from the first close
    # after the fund is invested.
    nav, bnav = r.nav[1:], r.bench[1:]
    ret = nav[1:] / nav[:-1] - 1
    bret = bnav[1:] / bnav[:-1] - 1
    rf = r.rf[2:]
    years = len(ret) / SESSIONS_A_YEAR
    act = ret - bret
    peak = np.maximum.accumulate(nav)
    reb = r.rebalances
    z = []
    for k, info in enumerate(reb):
        i0 = r.dates.index(info["date"])
        i1 = r.dates.index(reb[k + 1]["date"]) if k + 1 < len(reb) else len(r.dates) - 1
        a = act[max(i0 - 1, 0):i1 - 1]
        if len(a) and info["te_ex_ante"] > 1e-3:
            z.append(a.sum() / (info["te_ex_ante"] * np.sqrt(len(a) / SESSIONS_A_YEAR)))
    return {
        "strategy": r.config.strategy,
        "covariance": r.config.covariance,
        "start": r.dates[1], "end": r.dates[-1],
        "return": (nav[-1] / nav[0]) ** (1 / years) - 1,
        "vol": ret.std(ddof=1) * np.sqrt(SESSIONS_A_YEAR),
        "sharpe": (ret - rf).mean() / ret.std(ddof=1) * np.sqrt(SESSIONS_A_YEAR),
        "max_drawdown": float((nav / peak - 1).min()),
        "active_return": act.mean() * SESSIONS_A_YEAR,
        "tracking_error": act.std(ddof=1) * np.sqrt(SESSIONS_A_YEAR),
        "info_ratio": act.mean() / act.std(ddof=1) * np.sqrt(SESSIONS_A_YEAR)
        if act.std() > 0 else np.nan,
        "turnover": sum(i["turnover"] for i in reb) / years,
        "cost_bp": sum(i["cost"] for i in reb) / years * 1e4,
        "fee_bp": float(np.mean([i["fee"] for i in reb])) * 1e4,
        "names": float(np.mean([sum(w >= MIN_NAME_WEIGHT for w in i["weights"].values())
                                for i in reb])),
        "cash": float(np.mean([i["cash"] for i in reb])),
        "locked": float(np.mean([i["n_locked"] for i in reb])),
        "te_bias": float(np.std(z, ddof=1)) if len(z) > 2 else np.nan,
        "failed": sum(i["failed"] for i in reb),
        "projection": float(max(i["projection"] for i in reb)),
    }

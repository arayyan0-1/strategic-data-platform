"""Fixed weights held for the long run: short histories filled from long ones, return assumptions,
bootstrapped paths, three ways of holding, and the best weights at each level of risk."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace

import numpy as np
from scipy.optimize import minimize

TRADE_COST = 0.001
DRIFT_POINTS = 0.05
DRIFT_SHARE = 0.25
ASSUMPTIONS = ("history", "equal", "blend")
RULES = ("yearly", "never", "drift")
SIGMAS = (*(x / 100 for x in range(2, 21)), 0.25, 0.30, 0.40)
TAU_FLOOR = 0.05
MIN_OWN_YEARS = 20
# The drift objective has kinks where a rebalance date moves, so the solver can creep for
# hundreds of steps; it starts from the yearly answer and stops here.
DRIFT_MAXITER = 100


@dataclass
class History:
    """Monthly long histories of the pieces: ret is the gross return (rows months, columns pieces),
    NaN where a piece has no data; rf the T-bill return of each month; gap what the fund loses or
    gains against the history, a year, per piece."""

    names: list[str]
    kinds: list[str]
    months: list[dt.date]
    ret: np.ndarray
    rf: np.ndarray
    gap: np.ndarray

    def rows(self, keep: np.ndarray) -> History:
        return replace(self, months=[m for m, k in zip(self.months, keep, strict=True) if k],
                       ret=self.ret[keep], rf=self.rf[keep])

    def before(self, day: dt.date) -> History:
        return self.rows(np.array([m < day for m in self.months]))

    def since(self, day: dt.date) -> History:
        return self.rows(np.array([m >= day for m in self.months]))

    def cols(self, keep: list[int]) -> History:
        return replace(self, names=[self.names[j] for j in keep],
                       kinds=[self.kinds[j] for j in keep], ret=self.ret[:, keep],
                       gap=self.gap[keep])

    def col(self, name: str) -> int:
        return self.names.index(name)

    def own_months(self) -> np.ndarray:
        return (~np.isnan(self.ret)).sum(axis=0)


@dataclass
class Fill:
    """filled is the observed return where a piece has one, else the fit on the complete pieces;
    resid the regression residual where observed; pools the donor months of each group of
    filled pieces (columns, months)."""

    filled: np.ndarray
    beta: dict[int, np.ndarray]
    resid: np.ndarray
    pools: list[tuple[np.ndarray, np.ndarray]]


def fit_fill(h: History, regressors: list[str], solo: tuple[str, ...] = ("bitcoin",)) -> Fill:
    """Regress each piece with a missing month on the complete regressors over its own months.

    The filled pieces share one donor month, which keeps their residual correlations; a piece in
    solo draws its own. A pool holds the months where every piece of its group has a residual,
    and its residuals are centred on that pool.
    """
    reg = [h.col(r) for r in regressors]
    X = h.ret[:, reg]
    if np.isnan(X).any():
        raise ValueError("A regressor has a missing month.")
    Xc = np.column_stack([np.ones(len(X)), X])
    missing = np.isnan(h.ret)
    filled, resid, beta = h.ret.copy(), np.full_like(h.ret, np.nan), {}
    gaps = [j for j in range(h.ret.shape[1]) if missing[:, j].any()]
    for j in gaps:
        ok = ~missing[:, j]
        b = np.linalg.lstsq(Xc[ok], h.ret[ok, j], rcond=None)[0]
        fit = Xc @ b
        filled[~ok, j] = fit[~ok]
        resid[ok, j] = h.ret[ok, j] - fit[ok]
        beta[j] = b
    solos = [h.col(s) for s in solo if s in h.names and h.col(s) in gaps]
    groups = [np.array([j for j in gaps if j not in solos])] + [np.array([j]) for j in solos]
    pools = []
    for g in groups:
        if not len(g):
            continue
        months = np.flatnonzero(~np.isnan(resid[:, g]).any(axis=1))
        resid[np.ix_(months, g)] -= resid[np.ix_(months, g)].mean(axis=0)
        pools.append((g, months))
    return Fill(filled, beta, resid, pools)


def fitted(h: History, f: Fill, regressors: list[str]) -> np.ndarray:
    """The observed returns of h, with each missing month replaced by the fit of f."""
    X = np.column_stack([np.ones(len(h.ret)), h.ret[:, [h.col(r) for r in regressors]]])
    out = h.ret.copy()
    for j, b in f.beta.items():
        miss = np.isnan(out[:, j])
        out[miss, j] = X[miss] @ b
    return out


def rolling_vol(ret: np.ndarray, window: int = 12, log: bool = True) -> np.ndarray:
    """The standard deviation of each column's overlapping log returns over window months (or
    compounded returns when log is False), from the windows with no missing month."""
    out = np.full(ret.shape[1], np.nan)
    logs = np.log1p(ret)
    for j in range(ret.shape[1]):
        x = logs[:, j]
        sums = np.convolve(np.nan_to_num(x), np.ones(window), "valid")
        full = np.convolve((~np.isnan(x)).astype(float), np.ones(window), "valid") == window
        if full.sum() > 1:
            out[j] = np.std(sums[full] if log else np.expm1(sums[full]), ddof=1)
    return out


@dataclass
class Premia:
    """Annual excess returns over T-bills before fund costs, by assumption, and their inputs."""

    history: np.ndarray
    equal: np.ndarray
    blend: np.ndarray
    sigma: np.ndarray
    years: np.ndarray
    s: float
    tau: float

    def __getitem__(self, name: str) -> np.ndarray:
        return getattr(self, name)


def premia(h: History, f: Fill, log_vol: bool = True) -> Premia:
    """History is the mean excess return of the filled series (Stambaugh's estimator). equal gives
    every piece the median Sharpe ratio s of the pieces with MIN_OWN_YEARS of their own data.
    blend pulls each piece's Sharpe ratio toward s by the precision of its own record. Risk is
    the spread of 12-month log returns: compounded returns let a few years of extreme gains set
    the risk, and so the premium, of a skewed piece."""
    cash = np.array([k == "cash" for k in h.kinds])
    mu = 12 * np.mean(f.filled - h.rf[:, None], axis=0)
    sigma = rolling_vol(h.ret, log=log_vol)
    years = h.own_months() / 12
    sr = np.divide(mu, sigma, out=np.zeros_like(mu), where=~cash)
    risky = ~cash
    s = float(np.median(sr[risky & (years >= MIN_OWN_YEARS)]))
    tau2 = max(float(np.var(sr[risky], ddof=1) - np.mean(1 / years[risky])), TAU_FLOOR ** 2)
    blend_sr = (s / tau2 + sr * years) / (1 / tau2 + years)
    zero = np.where(cash, 0.0, 1.0)
    return Premia(history=mu * zero, equal=s * sigma * zero, blend=blend_sr * sigma * zero,
                  sigma=sigma, years=years, s=s, tau=float(np.sqrt(tau2)))


@dataclass
class Paths:
    """Bootstrapped gross returns (paths, months, pieces), the T-bill returns, and the mean
    monthly excess return of each piece in the filled panel, which the shift replaces."""

    gross: np.ndarray
    rf: np.ndarray
    base: np.ndarray

    def funds(self, excess: np.ndarray, gap: np.ndarray) -> np.ndarray:
        """Monthly fund returns with each piece's mean excess return moved to excess / 12, and
        the fund's gap added."""
        return self.gross + (excess / 12 - self.base + gap / 12)


def bootstrap(h: History, f: Fill, n_paths: int, n_months: int, block: float,
              rng: np.random.Generator) -> Paths:
    """A stationary bootstrap of months, mean block length block. A missing month of a piece
    takes its fit plus the residual of a donor month; donor months come in runs of 12 drawn
    afresh for each path."""
    T = len(h.months)
    idx = np.empty((n_paths, n_months), dtype=np.int64)
    idx[:, 0] = rng.integers(T, size=n_paths)
    for t in range(1, n_months):
        jump = rng.random(n_paths) < 1 / block
        idx[:, t] = np.where(jump, rng.integers(T, size=n_paths), (idx[:, t - 1] + 1) % T)
    gross = f.filled[idx]
    missing = np.isnan(h.ret)[idx]
    for cols, pool in f.pools:
        if len(pool) < 12:
            raise ValueError("A donor pool is shorter than a year.")
        starts = rng.integers(len(pool) - 11, size=(n_paths, -(-n_months // 12)))
        pos = (starts[:, :, None] + np.arange(12)).reshape(n_paths, -1)[:, :n_months]
        donors = f.resid[pool[pos]][:, :, cols]
        gross[:, :, cols] += np.where(missing[:, :, cols], donors, 0.0)
    base = np.mean(f.filled - h.rf[:, None], axis=0)
    return Paths(gross=gross, rf=h.rf[idx], base=base)


def simulate(R: np.ndarray, w: np.ndarray, rule: str, cost: float = TRADE_COST) -> np.ndarray:
    """Wealth at each month end (paths, months + 1), from 1, holding weights w under rule.

    yearly rebalances to w every 12 months; never buys w and holds; drift rebalances every piece
    when any is off by more than DRIFT_POINTS or DRIFT_SHARE of its target, the smaller. A
    rebalance costs cost per dollar traded.
    """
    S, T, _ = R.shape
    w = np.clip(w, 0, None)
    h = np.tile(w, (S, 1)).astype(float)
    V = np.empty((S, T + 1))
    V[:, 0] = 1.0
    limit = np.minimum(DRIFT_POINTS, DRIFT_SHARE * w)
    for t in range(T):
        h *= 1 + R[:, t, :]
        v = h.sum(axis=1)
        if rule == "yearly":
            due = np.full(S, (t + 1) % 12 == 0)
        elif rule == "drift":
            due = (np.abs(h / v[:, None] - w) > limit).any(axis=1)
        else:
            due = np.zeros(S, dtype=bool)
        if due.any():
            target = v[due, None] * w
            v[due] = v[due] - cost * np.abs(h[due] - target).sum(axis=1)
            h[due] = v[due, None] * w
        V[:, t + 1] = v
    return V


def measures(V: np.ndarray) -> dict:
    """Growth, risk and losses of monthly wealth paths."""
    years = (V.shape[1] - 1) // 12
    log_w = np.log(V[:, -1])
    yearly = V[:, 12::12] / V[:, :-1:12][:, :years] - 1
    peak = np.maximum.accumulate(V, axis=1)
    return {"growth": float(log_w.mean() / years),
            "growth_median": float(np.expm1(np.median(log_w) / years)),
            "growth_p5": float(np.expm1(np.quantile(log_w, 0.05) / years)),
            "risk": float(yearly.std()),
            "max_drawdown_median": float(np.median((V / peak - 1).min(axis=1)))}


class Yearly:
    """Growth and risk of rebalancing once a year, from yearly gross returns G (paths, years,
    pieces), with exact gradients."""

    def __init__(self, R: np.ndarray, cost: float = TRADE_COST):
        S, T, N = R.shape
        self.G = np.prod(1 + R[:, : T // 12 * 12].reshape(S, T // 12, 12, N), axis=2)
        self.k = cost

    def _net(self, w):
        g = self.G @ w
        dev = self.G - g[..., None]
        net = g - self.k * (np.abs(dev) @ w)
        dnet = (self.G - self.k * np.abs(dev)
                + self.k * self.G * (np.sign(dev) @ w)[..., None])
        return net, dnet

    def growth(self, w):
        net, dnet = self._net(w)
        return np.log(net).mean(), (dnet / net[..., None]).mean(axis=(0, 1))

    def risk(self, w):
        net, dnet = self._net(w)
        m = net.mean()
        return (net ** 2).mean() - m ** 2, 2 * ((net[..., None] * dnet).mean(axis=(0, 1))
                                               - m * dnet.mean(axis=(0, 1)))


class Never:
    """Growth and risk of buying once and never trading, with exact gradients."""

    def __init__(self, R: np.ndarray):
        S, T, N = R.shape
        G = np.prod(1 + R[:, : T // 12 * 12].reshape(S, T // 12, 12, N), axis=2)
        self.P = np.concatenate([np.ones((S, 1, N)), np.cumprod(G, axis=1)], axis=1)
        self.years = G.shape[1]

    def growth(self, w):
        v = self.P[:, -1] @ w
        return np.log(v).mean() / self.years, (self.P[:, -1] / v[:, None]).mean(0) / self.years

    def risk(self, w):
        V = self.P @ w
        a = V[:, 1:] / V[:, :-1]
        da = (self.P[:, 1:] * V[:, :-1, None] - V[:, 1:, None] * self.P[:, :-1]) \
            / V[:, :-1, None] ** 2
        m = a.mean()
        return (a ** 2).mean() - m ** 2, 2 * ((a[..., None] * da).mean(axis=(0, 1))
                                             - m * da.mean(axis=(0, 1)))


class Drift:
    """Growth and risk of rebalancing on drift. The gradients are pathwise: they hold the dates
    of each path's rebalances fixed, which is exact between the weights where a date changes."""

    def __init__(self, R: np.ndarray, cost: float = TRADE_COST):
        self.R, self.k = R, cost
        self._w, self._m = None, None

    def _years(self, w):
        """Wealth at each year end (paths, years + 1) and its gradient in w."""
        R, k = self.R, self.k
        S, T, N = R.shape
        h = np.tile(w, (S, 1)).astype(float)
        J = np.tile(np.eye(N), (S, 1, 1))
        limit = np.minimum(DRIFT_POINTS, DRIFT_SHARE * w)
        V = [np.ones(S)]
        dV = [np.zeros((S, N))]
        for t in range(T):
            h *= 1 + R[:, t, :]
            J *= (1 + R[:, t, :])[:, :, None]
            v = h.sum(axis=1)
            due = (np.abs(h / v[:, None] - w) > limit).any(axis=1)
            if due.any():
                hd, Jd, vd = h[due], J[due], v[due]
                dv = Jd.sum(axis=1)
                dev = hd - vd[:, None] * w
                sgn = np.sign(dev)
                ddev = Jd - dv[:, None, :] * w[None, :, None] - vd[:, None, None] * np.eye(N)
                v2 = vd - k * np.abs(dev).sum(axis=1)
                dv2 = dv - k * np.einsum("si,sim->sm", sgn, ddev)
                h[due] = v2[:, None] * w
                J[due] = dv2[:, None, :] * w[None, :, None] + v2[:, None, None] * np.eye(N)
            if (t + 1) % 12 == 0:
                V.append(h.sum(axis=1))
                dV.append(J.sum(axis=1))
        return np.stack(V, axis=1), np.stack(dV, axis=1)

    def _measures(self, w):
        if self._w is None or not np.array_equal(w, self._w):
            V, dV = self._years(np.clip(w, 0, None))
            years = V.shape[1] - 1
            growth = np.log(V[:, -1]).mean() / years
            dgrowth = (dV[:, -1] / V[:, -1, None]).mean(axis=0) / years
            a = V[:, 1:] / V[:, :-1]
            da = (dV[:, 1:] * V[:, :-1, None] - V[:, 1:, None] * dV[:, :-1]) / V[:, :-1, None] ** 2
            m = a.mean()
            risk = (a ** 2).mean() - m ** 2
            drisk = 2 * ((a[..., None] * da).mean(axis=(0, 1)) - m * da.mean(axis=(0, 1)))
            self._w, self._m = w.copy(), ((growth, dgrowth), (risk, drisk))
        return self._m

    def growth(self, w):
        return self._measures(w)[0]

    def risk(self, w):
        return self._measures(w)[1]


def holder(R: np.ndarray, rule: str, cost: float = TRADE_COST):
    return {"yearly": lambda: Yearly(R, cost), "never": lambda: Never(R),
            "drift": lambda: Drift(R, cost)}[rule]()


def _clean(w: np.ndarray) -> np.ndarray:
    w = np.where(w < 1e-5, 0.0, w)
    return w / w.sum()


def solve(model, n: int, sigma: float | None = None, w0: np.ndarray | None = None,
          minimum_risk: bool = False, allowed: np.ndarray | None = None,
          maxiter: int = 1000) -> tuple[np.ndarray, bool]:
    """The long-only weights summing to 1 with the most growth at risk sigma or less (no limit
    when sigma is None), or the least risk, holding only the allowed pieces. Returns the weights
    and whether the solver converged."""
    allowed = np.ones(n, dtype=bool) if allowed is None else np.asarray(allowed, dtype=bool)
    w0 = np.where(allowed, 1.0, 0.0) if w0 is None else np.where(allowed, w0, 0.0)
    w0 = w0 / w0.sum() if w0.sum() > 0 else np.where(allowed, 1.0, 0.0) / allowed.sum()
    cons = [{"type": "eq", "fun": lambda w: w.sum() - 1, "jac": lambda w: np.ones(n)}]
    jac = model.growth(w0)[1] is not None
    sign, part = (1.0, model.risk) if minimum_risk else (-1.0, model.growth)

    def fun(w):
        return sign * part(w)[0]

    def grad(w):
        return sign * part(w)[1]

    if not minimum_risk and sigma is not None:
        cons.append({"type": "ineq", "fun": lambda w: sigma ** 2 - model.risk(w)[0],
                     **({"jac": lambda w: -model.risk(w)[1]} if jac else {})})
    bounds = [(0.0, 1.0 if a else 0.0) for a in allowed]
    r = minimize(fun, w0, jac=grad if jac else None, bounds=bounds, constraints=cons,
                 method="SLSQP", options={"ftol": 1e-12, "maxiter": maxiter})
    return _clean(np.clip(r.x, 0, None)), bool(r.success)


def frontier(model, n: int, sigmas=SIGMAS, starts: dict | None = None,
             allowed: np.ndarray | None = None, maxiter: int = 1000) -> list[dict]:
    """The best weights at each risk limit, from the least-risk portfolio up, then with no limit.
    starts holds a starting point per limit (another rule's answer); otherwise each solve starts
    from the one before."""
    w_min, ok = solve(model, n, minimum_risk=True, allowed=allowed, maxiter=maxiter,
                      w0=None if starts is None else starts.get("min"))
    floor = float(np.sqrt(model.risk(w_min)[0]))
    out = [{"limit": "min", "weights": w_min, "converged": ok}]
    w = w_min
    for s in (*sigmas, None):
        if s is not None and s < floor:
            out.append({"limit": s, "weights": None, "converged": False})
            continue
        w0 = starts.get(s, w) if starts else w
        w, ok = solve(model, n, sigma=s, w0=w0, allowed=allowed, maxiter=maxiter)
        out.append({"limit": s, "weights": w, "converged": ok})
    return out


REGRESSORS = ("us_market", "us_small", "us_value", "ust_5y", "ust_10y", "corporate", "gold",
              "silver", "copper", "agriculture")
VT_MIX = {"us_market": 0.66, "europe": 0.14, "japan": 0.05, "pacific": 0.03, "emerging": 0.12}
SIXTY_FORTY = ("ust_5y", 0.40)
MIN_OWN_MONTHS = 60


def references(names: list[str]) -> dict[str, np.ndarray]:
    """The VT stand-in (today's ACWI mix, renormalized over the pieces present) and 60/40."""
    vt = np.array([VT_MIX.get(n, 0.0) for n in names])
    vt = vt / vt.sum()
    bond, share = SIXTY_FORTY
    sf = (1 - share) * vt
    sf[names.index(bond)] += share
    return {"vt": vt, "sixty_forty": sf}


def _point(R, w, rule, limit, converged=True) -> dict:
    out = {"limit": limit, "converged": converged,
           "weights": None if w is None else [float(x) for x in w]}
    if w is not None:
        out.update(measures(simulate(R, w, rule)))
    return out


def ranges(h: History, paths: Paths, prem: Premia, assumptions=ASSUMPTIONS,
           rules_for: dict | None = None, sigmas=SIGMAS, levels=(1.0, 2 / 3, 1 / 3),
           allowed: np.ndarray | None = None) -> dict:
    """For each assumption: the references, the full range of best weights under each rule, and
    the best weights at the VT stand-in's risk and fractions of it (levels).

    rules_for maps an assumption to its rules (all three by default). never and drift start from
    the yearly answer at the same limit. Only the allowed pieces are held.
    """
    n = len(h.names)
    refs = references(h.names)
    out = {}
    for a in assumptions:
        R = paths.funds(prem[a], h.gap)
        yearly = Yearly(R)
        ref = {k: measures(simulate(R, w, "yearly")) for k, w in refs.items()}
        vt_risk = float(np.sqrt(yearly.risk(refs["vt"])[0]))
        result = {"references": ref, "vt_risk": vt_risk, "frontiers": {}, "levels": {}}
        base: dict = {}
        for rule in (rules_for or {}).get(a, RULES):
            model = yearly if rule == "yearly" else holder(R, rule)
            cap = DRIFT_MAXITER if rule == "drift" else 1000
            pts = frontier(model, n, sigmas, starts=base or None, allowed=allowed, maxiter=cap)
            if rule == "yearly":
                base = {p["limit"]: p["weights"] for p in pts if p["weights"] is not None}
            result["frontiers"][rule] = [_point(R, p["weights"], rule, p["limit"], p["converged"])
                                         for p in pts]
            lv = []
            for f in levels:
                w0 = refs["vt"] if rule == "yearly" else base.get(("level", f))
                w, ok = solve(model, n, sigma=f * vt_risk, w0=w0, allowed=allowed,
                              maxiter=cap)
                if rule == "yearly":
                    base[("level", f)] = w
                lv.append({"fraction": f, **_point(R, w, rule, f * vt_risk, ok)})
            result["levels"][rule] = lv
        out[a] = result
    return out


def walk_forward(h: History, years=tuple(range(1992, 2017, 2)), n_paths: int = 500,
                 horizon: int = 20, hold: int = 10, block: float = 24.0,
                 seed: int = 20261014, allowed_kinds: tuple[str, ...] | None = None
                 ) -> list[dict]:
    """Fit on the months before each January of years, choose the best weights at the VT
    stand-in's risk under each assumption and rule, and hold them for hold years of actual
    returns. A piece needs MIN_OWN_MONTHS of its own data before the start, and a kind in
    allowed_kinds (any kind when None) to be held."""
    rng = np.random.default_rng(seed)
    rows = []
    for year in years:
        day = dt.date(year, 1, 1)
        fit = h.before(day)
        keep = [j for j in range(len(h.names)) if fit.own_months()[j] >= MIN_OWN_MONTHS]
        fit = fit.cols(keep)
        f = fit_fill(fit, list(REGRESSORS))
        prem = premia(fit, f)
        paths = bootstrap(fit, f, n_paths, 12 * horizon, block, rng)
        test = h.since(day).cols(keep)
        test = test.rows(np.arange(len(test.months)) < 12 * hold)
        real = (fitted(test, f, list(REGRESSORS)) + test.gap / 12)[None]
        refs = references(fit.names)
        allowed = np.array([allowed_kinds is None or k in allowed_kinds for k in fit.kinds])
        actual = {k: measures(simulate(real, w, "yearly")) for k, w in refs.items()}
        for a in ASSUMPTIONS:
            R = paths.funds(prem[a], fit.gap)
            yearly = Yearly(R)
            target = float(np.sqrt(yearly.risk(refs["vt"])[0]))
            wy, ok = solve(yearly, len(keep), sigma=target, w0=refs["vt"], allowed=allowed)
            for rule in RULES:
                cap = DRIFT_MAXITER if rule == "drift" else 1000
                w = wy if rule == "yearly" else solve(holder(R, rule), len(keep), target, wy,
                                                      allowed=allowed, maxiter=cap)[0]
                rows.append({"start": year, "assumption": a, "rule": rule, "target": target,
                             "pieces": fit.names, "weights": [float(x) for x in w],
                             "converged": ok, "realized": measures(simulate(real, w, rule)),
                             "vt": actual["vt"], "sixty_forty": actual["sixty_forty"]})
    return rows


def run_ranges(h: History, seed: int, n_paths: int, years: int, block: float,
               log_vol: bool = True, **kw) -> tuple[Premia, dict]:
    """Fill, assume, bootstrap from seed and find the ranges: one unit of work for a pool."""
    f = fit_fill(h, list(REGRESSORS))
    prem = premia(h, f, log_vol=log_vol)
    paths = bootstrap(h, f, n_paths, 12 * years, block, np.random.default_rng(seed))
    return prem, ranges(h, paths, prem, **kw)


def drift_points(h: History, seed: int, n_paths: int, years: int, block: float, assumption: str,
                 limits: list, allowed: np.ndarray | None = None) -> list[dict]:
    """The drift rule's best weights at some limits, each solved from the yearly answer at the
    same limit, as ranges does: one unit of work for a pool. A limit is "min", a risk, None (no
    limit) or ("level", f), a fraction f of the VT stand-in's risk."""
    f = fit_fill(h, list(REGRESSORS))
    prem = premia(h, f)
    paths = bootstrap(h, f, n_paths, 12 * years, block, np.random.default_rng(seed))
    R = paths.funds(prem[assumption], h.gap)
    n = len(h.names)
    yearly = Yearly(R)
    base = {p["limit"]: p["weights"] for p in frontier(yearly, n, SIGMAS, allowed=allowed)
            if p["weights"] is not None}
    vt = references(h.names)["vt"]
    vt_risk = float(np.sqrt(yearly.risk(vt)[0]))
    floor = float(np.sqrt(yearly.risk(base["min"])[0]))
    drift = Drift(R)
    out = []
    for limit in limits:
        if isinstance(limit, tuple):
            sigma = limit[1] * vt_risk
            start = solve(yearly, n, sigma=sigma, w0=vt, allowed=allowed)[0]
        elif limit == "min":
            sigma, start = None, base["min"]
        else:
            sigma, start = limit, base.get(limit)
        if start is None or (sigma is not None and sigma < floor):
            out.append({"limit": limit, "weights": None, "converged": False})
            continue
        w, ok = solve(drift, n, sigma=sigma, w0=start, allowed=allowed,
                      minimum_risk=limit == "min", maxiter=DRIFT_MAXITER)
        point = _point(R, w, "drift", sigma if isinstance(limit, tuple) else limit, ok)
        if isinstance(limit, tuple):
            point["fraction"] = limit[1]
        out.append(point)
    return out

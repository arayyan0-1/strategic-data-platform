"""Features, walk-forward fits and scores of the relative return of an index. No I/O.

A month is an integer: months since 1970-01. A panel row is an index (or fund) at the end of month
t; its target is the return of month t + 1 less the mean over its universe. Arrays are rows of the
panel, with the columns of the raw features in the order of FEATURES.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
from numpy.lib.stride_tricks import sliding_window_view
from sklearn.ensemble import HistGradientBoostingRegressor

from sdp.evaluation import newey_west

FEATURES = ("r3", "r6", "mom", "val", "vol12", "dd12", "trend")
UNIVERSES = ("regions", "countries", "us_segments")
MODELS = ("ridge", "gbt")
BASELINES = ("combo", "mom")
PENALTIES = (0.1, 1.0, 10.0, 100.0, 1000.0)
VALID_SHARE = 0.2
GBT_PARAMS = {
    "max_depth": 3, "learning_rate": 0.05, "max_iter": 200, "min_samples_leaf": 50,
    "l2_regularization": 1.0, "random_state": 0,
    # The default stops early on a random tenth once there are 10,000 rows, and a random tenth
    # of a panel of months is not a later period.
    "early_stopping": False,
}
FIRST_YEAR = 1996
LAG = 5
MIN_NAMES = 5


def month_index(dates: np.ndarray) -> np.ndarray:
    """Return months since 1970-01 for an array of dates."""
    return np.asarray(dates).astype("datetime64[M]").astype(np.int64)


# ---------- features ----------

def _window(x: np.ndarray, back: int, lag: int = 0) -> np.ndarray:
    """Return row t = x[t-back .. t-lag], NaN before the start."""
    padded = np.r_[np.full(back, np.nan), x]
    return sliding_window_view(padded, back - lag + 1)[: x.size]


def features(ret: np.ndarray, rf: np.ndarray) -> dict[str, np.ndarray]:
    """Return the features at the end of each month of one series of consecutive monthly returns.

    r3, r6: returns of the last 3 and 6 months. mom: months t-11 to t-1. val: minus the return of
    months t-59 to t-12. vol12: standard deviation of the last 12 returns. dd12: the largest
    fall from a peak of the wealth path of those 12 months, starting at 1 (0 or below). trend:
    the 12-month return less the compounded rf. A window with a missing return gives NaN.
    """
    ret = np.asarray(ret, dtype=float)
    rf = np.asarray(rf, dtype=float)
    last12 = 1.0 + _window(ret, 11)
    wealth = np.cumprod(last12, axis=1)
    path = np.column_stack([np.ones(ret.size), wealth])
    peak = np.maximum.accumulate(path, axis=1)
    return {
        "r3": np.prod(1.0 + _window(ret, 2), axis=1) - 1.0,
        "r6": np.prod(1.0 + _window(ret, 5), axis=1) - 1.0,
        "mom": np.prod(1.0 + _window(ret, 11, 1), axis=1) - 1.0,
        "val": 1.0 - np.prod(1.0 + _window(ret, 59, 12), axis=1),
        "vol12": np.std(_window(ret, 11), axis=1, ddof=1),
        "dd12": np.min(path / peak - 1.0, axis=1),
        "trend": np.prod(last12, axis=1) - np.prod(1.0 + _window(rf, 11), axis=1),
    }


def average_rank(x: np.ndarray) -> np.ndarray:
    """Return the ranks of x from 1, a tie taking the mean of its ranks."""
    _, inverse, counts = np.unique(x, return_inverse=True, return_counts=True)
    return (np.cumsum(counts) - (counts - 1) / 2.0)[inverse]


def rank_scale(x: np.ndarray, group: np.ndarray) -> np.ndarray:
    """Return the rank of each value within its group, scaled to [-0.5, 0.5].

    The lowest value of a group is -0.5 and the highest 0.5. A value alone in its group is 0. A
    missing value stays NaN.
    """
    x = np.asarray(x, dtype=float)
    group = np.asarray(group)
    out = np.full(x.shape, np.nan)
    ok = np.flatnonzero(np.isfinite(x))
    order = ok[np.argsort(group[ok], kind="stable")]
    g = group[order]
    cuts = np.flatnonzero(np.r_[True, g[1:] != g[:-1], True])
    for a, b in zip(cuts[:-1], cuts[1:], strict=True):
        members = order[a:b]
        out[members] = 0.0 if b - a == 1 else (average_rank(x[members]) - 1) / (b - a - 1) - 0.5
    return out


def matrix(raw: np.ndarray, universe: np.ndarray, month: np.ndarray) -> np.ndarray:
    """Return the model inputs: raw features, their ranks and one indicator per universe.

    raw has one column per FEATURES entry, NaN where missing. universe holds the index of the
    universe in UNIVERSES. A rank is taken within the universe and month, and is 0 where the
    feature is missing. A missing raw value stays NaN until the standardizing sets it to 0.
    """
    raw = np.asarray(raw, dtype=float)
    universe = np.asarray(universe).astype(np.int64)
    group = universe * 1_000_000 + np.asarray(month).astype(np.int64)
    ranks = np.column_stack([rank_scale(raw[:, j], group) for j in range(raw.shape[1])])
    indicators = (universe[:, None] == np.arange(len(UNIVERSES))).astype(float)
    return np.column_stack([raw, np.nan_to_num(ranks, nan=0.0), indicators])


# ---------- models ----------

@dataclass
class Standardizer:
    """Centre and scale of each column, from the finite values of the rows it was fitted on."""

    mean: np.ndarray
    scale: np.ndarray

    @classmethod
    def fit(cls, X: np.ndarray) -> Standardizer:
        ok = np.isfinite(X)
        n = ok.sum(axis=0)
        filled = np.where(ok, X, 0.0)
        mean = np.where(n > 0, filled.sum(axis=0) / np.maximum(n, 1), 0.0)
        var = np.where(n > 0, (np.where(ok, X - mean, 0.0) ** 2).sum(axis=0) / np.maximum(n, 1),
                       0.0)
        sd = np.sqrt(var)
        return cls(mean, np.where(sd > 0, sd, 1.0))

    def apply(self, X: np.ndarray) -> np.ndarray:
        """Return the standardized columns, a missing value set to 0 (the mean)."""
        return np.nan_to_num((X - self.mean) / self.scale, nan=0.0)


def _ridge(Z: np.ndarray, y: np.ndarray, penalty: float) -> tuple[np.ndarray, float]:
    """Return the coefficients and the intercept of ridge with the penalty on the sum of squares."""
    zbar, ybar = Z.mean(axis=0), y.mean()
    Zc = Z - zbar
    w = np.linalg.solve(Zc.T @ Zc + penalty * np.eye(Z.shape[1]), Zc.T @ (y - ybar))
    return w, float(ybar - zbar @ w)


def choose_penalty(X: np.ndarray, y: np.ndarray, month: np.ndarray,
                   penalties: tuple[float, ...] = PENALTIES, share: float = VALID_SHARE) -> float:
    """Return the penalty with the lowest squared error on the last `share` of the months.

    The fit part is standardized on itself. A tie goes to the smaller penalty.
    """
    months = np.unique(month)
    if months.size < 5:
        return float(penalties[len(penalties) // 2])
    cut = months[months.size - max(1, round(share * months.size))]
    fit_rows, valid_rows = month < cut, month >= cut
    scaler = Standardizer.fit(X[fit_rows])
    Zf, Zv = scaler.apply(X[fit_rows]), scaler.apply(X[valid_rows])
    errors = []
    for penalty in penalties:
        w, b = _ridge(Zf, y[fit_rows], penalty)
        errors.append(np.mean((y[valid_rows] - (Zv @ w + b)) ** 2))
    return float(penalties[int(np.argmin(errors))])


@dataclass
class Fit:
    """A fitted model: it holds the standardizer of its training rows."""

    kind: str
    scaler: Standardizer
    estimator: object
    penalty: float | None = None

    def predict(self, X: np.ndarray) -> np.ndarray:
        Z = self.scaler.apply(X)
        if self.kind == "ridge":
            w, b = self.estimator
            return Z @ w + b
        return self.estimator.predict(Z)


def fit(kind: str, X: np.ndarray, y: np.ndarray, month: np.ndarray) -> Fit:
    """Fit a model of MODELS on the rows of X (the output of `matrix`) with targets y."""
    if kind == "ridge":
        penalty = choose_penalty(X, y, month)
        scaler = Standardizer.fit(X)
        return Fit(kind, scaler, _ridge(scaler.apply(X), y, penalty), penalty)
    if kind == "gbt":
        scaler = Standardizer.fit(X)
        model = HistGradientBoostingRegressor(**GBT_PARAMS).fit(scaler.apply(X), y)
        return Fit(kind, scaler, model)
    raise ValueError(f"unknown model {kind!r}")


# ---------- walk-forward ----------

def refit_plan(month: np.ndarray, y: np.ndarray, first_year: int, last_year: int):
    """Yield (year, train, test) masks for each calendar year from first_year to last_year.

    The model of year Y trains on the rows with a target whose target month (month + 1) is before
    January of Y, so the month before it is purged, and predicts the rows with month in Y. A year
    with no training row or no row to predict is skipped.
    """
    month = np.asarray(month)
    for year in range(first_year, last_year + 1):
        first = (year - 1970) * 12
        train = np.isfinite(y) & (month + 1 < first)
        test = (month >= first) & (month < first + 12)
        if train.any() and test.any():
            yield year, train, test


def walk_forward(X: np.ndarray, y: np.ndarray, month: np.ndarray, first_year: int,
                 last_year: int, models: tuple[str, ...] = MODELS,
                 fit_fn: Callable[..., Fit] | None = None):
    """Return the out-of-sample predictions of each model and the ridge penalty of each year.

    The predictions are NaN outside the years first_year to last_year. Each model is refitted once
    a year on the rows of refit_plan.
    """
    fit_fn = fit_fn or fit
    preds = {k: np.full(len(y), np.nan) for k in models}
    penalties: dict[int, float] = {}
    for year, train, test in refit_plan(month, y, first_year, last_year):
        for kind in models:
            model = fit_fn(kind, X[train], y[train], month[train])
            preds[kind][test] = model.predict(X[test])
            if model.penalty is not None:
                penalties[year] = model.penalty
    return preds, penalties


# ---------- scores ----------

def combo_score(mom: np.ndarray, val: np.ndarray, month: np.ndarray) -> np.ndarray:
    """Return the mean of the z-scores of mom and val within each month (sample deviation).

    The rows passed set the mean and deviation, and a row needs both signals. NaN where a month
    has fewer than 2 rows or a deviation of 0.
    """
    mom, val, month = (np.asarray(a) for a in (mom, val, month))
    out = np.full(mom.shape, np.nan)
    for m in np.unique(month):
        idx = np.flatnonzero((month == m) & np.isfinite(mom) & np.isfinite(val))
        if idx.size < 2:
            continue
        sd_m, sd_v = mom[idx].std(ddof=1), val[idx].std(ddof=1)
        if sd_m > 0 and sd_v > 0:
            out[idx] = ((mom[idx] - mom[idx].mean()) / sd_m
                        + (val[idx] - val[idx].mean()) / sd_v) / 2
    return out


def monthly_stats(score: np.ndarray, ret: np.ndarray, month: np.ndarray,
                  min_names: int = MIN_NAMES):
    """Return (months, n_names, ic, spread) for the months with min_names finite pairs or more.

    ic is the Spearman correlation of the score with ret (ties take mean ranks). spread is the mean
    ret of the top third of names by score less the bottom third, as the SQL ntile(3) cuts them.
    """
    score, ret, month = (np.asarray(a) for a in (score, ret, month))
    ok = np.flatnonzero(np.isfinite(score) & np.isfinite(ret))
    order = ok[np.argsort(month[ok], kind="stable")]
    cuts = np.flatnonzero(np.r_[True, month[order][1:] != month[order][:-1], True])
    out: dict[str, list] = {"month": [], "n": [], "ic": [], "spread": []}
    for a, b in zip(cuts[:-1], cuts[1:], strict=True):
        n = b - a
        if n < min_names:
            continue
        idx = order[a:b]
        s, r = score[idx], ret[idx]
        rs, rr = average_rank(s), average_rank(r)
        ic = np.nan if rs.std() == 0 or rr.std() == 0 else np.corrcoef(rs, rr)[0, 1]
        by_score = r[np.argsort(s, kind="stable")]
        bottom = by_score[: n // 3 + (n % 3 > 0)]
        top = by_score[n - n // 3:]
        out["month"].append(month[idx[0]])
        out["n"].append(n)
        out["ic"].append(ic)
        out["spread"].append(top.mean() - bottom.mean())
    return (np.array(out["month"], dtype=np.int64), np.array(out["n"], dtype=float),
            np.array(out["ic"], dtype=float), np.array(out["spread"], dtype=float))


def _t(x: np.ndarray) -> tuple[float, float]:
    mean, se = newey_west(x, LAG)
    return mean, (mean / se if se > 0 else np.nan)


def evaluate(scores: dict[str, np.ndarray], target: np.ndarray, month: np.ndarray
             ) -> dict[str, dict[str, float]]:
    """Return the statistics of each score over the same rows.

    Per score: first and last month, n_months (months with an IC), mean_names, mean_ic, t_nw (the
    Newey-West t of the IC, lag 5), spread_ann (the tercile spread times 12) and spread_t_nw. Pass
    only the rows that every score has, so all scores see the same months and names.
    """
    out = {}
    for name, score in scores.items():
        months, n, ic, spread = monthly_stats(score, target, month)
        mean_ic, t_ic = _t(ic)
        mean_sp, t_sp = _t(spread)
        out[name] = {
            "first_month": int(months.min()) if months.size else None,
            "last_month": int(months.max()) if months.size else None,
            "n_months": int(np.isfinite(ic).sum()),
            "mean_names": float(n.mean()) if n.size else np.nan,
            "mean_ic": mean_ic, "t_nw": t_ic, "spread_ann": mean_sp * 12, "spread_t_nw": t_sp,
        }
    return out

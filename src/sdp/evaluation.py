"""The evaluation protocol of a signal: which labels a period may use, and standard
errors that allow for overlapping labels. NumPy only, no I/O.

mart_eval_sessions gives each session a fold: 0 before development, 1 to K for the
development folds in date order, and K + 1 for the holdout. The label of a session with
horizon h ends h sessions later, in the fold label_fold. A period uses a session only
when the session and the end of its label are both inside the period, so no label
reaches into the next fold or into the holdout.
"""
from __future__ import annotations

import numpy as np


def nw_lag(horizon: int) -> int:
    """Return the Newey-West lag for a label of `horizon` sessions: twice the horizon, and
    at least five sessions. It covers the overlap of the labels and the persistence of
    the factor returns after it."""
    return max(2 * horizon, 5)


def inside(fold: np.ndarray, label_fold: np.ndarray, members) -> np.ndarray:
    """Return True for each session that a period of the folds in `members` may use: the
    session and the end of its label are both in the period. A label that ends after the
    last session has no fold (-1 or NaN) and is never inside."""
    members = np.asarray(list(members))
    return np.isin(fold, members) & np.isin(label_fold, members)


def walk_forward(fold: np.ndarray, label_fold: np.ndarray, n_folds: int):
    """Yield (k, train, test) for each development fold k from 2 to n_folds. train holds
    the sessions of the folds before k whose label ends before fold k, and test the
    sessions of fold k whose label ends in fold k. Training data lies before the test
    fold, so no embargo after the test fold is necessary."""
    for k in range(2, n_folds + 1):
        yield k, inside(fold, label_fold, range(1, k)), inside(fold, label_fold, [k])


def newey_west(x: np.ndarray, lag: int) -> tuple[float, float]:
    """Return the mean of x and its standard error with Bartlett weights up to `lag`.
    With lag 0 this is the standard error of independent values. A value that is not
    finite is left out."""
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    n = x.size
    if n < 2:
        return (float(x.mean()) if n else np.nan), np.nan
    d = x - x.mean()
    s = d @ d / n
    for k in range(1, min(lag, n - 1) + 1):
        s += 2.0 * (1.0 - k / (lag + 1)) * (d[k:] @ d[:-k]) / n
    return float(x.mean()), float(np.sqrt(max(s, 0.0) / n))


def ic_stats(ic: np.ndarray, horizon: int) -> dict[str, float]:
    """Return the statistics of a daily IC series in date order: the count, the mean,
    the standard deviation, the t that assumes independent days, the Newey-West t and
    the lag-one autocorrelation."""
    ic = np.asarray(ic, dtype=float)
    ic = ic[np.isfinite(ic)]
    n = ic.size
    out = {"n_days": float(n), "mean_ic": np.nan, "ic_std": np.nan, "t_naive": np.nan,
           "t_nw": np.nan, "ic_autocorr_lag1": np.nan}
    if n < 3:
        return out
    mean, se = newey_west(ic, nw_lag(horizon))
    std = ic.std(ddof=1)
    out.update(mean_ic=mean, ic_std=std)
    if std > 0:
        out["t_naive"] = mean / (std / np.sqrt(n))
        out["ic_autocorr_lag1"] = float(np.corrcoef(ic[1:], ic[:-1])[0, 1])
    if se > 0:
        out["t_nw"] = mean / se
    return out


STATS = ("n_days", "mean_ic", "ic_std", "t_naive", "t_nw", "ic_autocorr_lag1")


def summarize(group, horizon, ic, fold, label_fold, session,
              periods: dict[int, list[int]]) -> dict[str, np.ndarray]:
    """Return one row for each group and period with the statistics of ic_stats. The
    input rows are sorted by group and date. group, horizon, fold, label_fold and
    session are integer arrays, and a label with no fold is -1. periods maps a period
    code to its folds. first and last are the first and last session that the period
    uses, or -1 when it uses none."""
    group, horizon = np.asarray(group), np.asarray(horizon)
    ic, fold = np.asarray(ic, dtype=float), np.asarray(fold)
    label_fold, session = np.asarray(label_fold), np.asarray(session)
    starts = np.flatnonzero(np.r_[True, group[1:] != group[:-1]]) if group.size else []
    ends = np.r_[starts[1:], group.size] if group.size else []
    out: dict[str, list] = {c: [] for c in ("group", "period", "first", "last", *STATS)}
    for a, b in zip(starts, ends, strict=True):
        for code, members in periods.items():
            use = inside(fold[a:b], label_fold[a:b], members)
            used = session[a:b][use]
            out["group"].append(group[a])
            out["period"].append(code)
            out["first"].append(used.min() if used.size else -1)
            out["last"].append(used.max() if used.size else -1)
            stats = ic_stats(ic[a:b][use], int(horizon[a]))
            for c in STATS:
                out[c].append(stats[c])
    types = {"group": np.int64, "period": np.int64, "first": np.int64, "last": np.int64}
    return {c: np.asarray(v, dtype=types.get(c, float)) for c, v in out.items()}

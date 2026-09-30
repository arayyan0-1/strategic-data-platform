# src/sdp/factors.py
"""Factor-mimicking portfolios from a panel of returns. NumPy only, with no I/O. The dbt
Python models read the warehouse and call these functions.

Four constructions:

- style_returns: a cross-sectional regression on each session. Each coefficient is the
  return of a portfolio with unit exposure to one characteristic and zero exposure to
  the others (Fama-MacBeth, as in a Barra model).
- pca_returns: eigenportfolios of the trailing correlation matrix, formed at each
  rebalance and held out of sample.
- mimic_returns: a rolling projection of a target return series (a published factor) on
  a set of traded base portfolios, held out of sample.
- fund_loadings: a rolling ridge regression of the return of each fund on the factor
  returns, held out of sample. The loadings are the exposures of a security that the
  cross-section cannot describe from its characteristics.

A row of a panel is a session and a column is a name. A return on row d is the return
from the close of session d - 1 to the close of session d.
"""
from __future__ import annotations

import numpy as np

from sdp.risk import eligible


def filled(a) -> np.ndarray:
    """Return a float array with NaN where a masked array from DuckDB has a NULL."""
    return np.ma.filled(np.ma.asarray(a, dtype=float), np.nan)


# The warehouse expression of each style characteristic. The alias u is int_universe and
# the alias s is signals. A characteristic that the log cannot take is null.
STYLE_SQL = {
    "size": "case when u.market_cap > 0 then ln(u.market_cap) end",
    "liquidity": "case when u.adv > 0 and u.market_cap > 0 then ln(u.adv / u.market_cap) end",
    "beta": "s.beta_252",
    "momentum": "s.momentum_12_1",
    "reversal": "s.reversal_5",
    "volatility": "s.vol_60",
    "dividend_yield": "s.dividend_yield",
    "high_52w": "s.high_52w",
}


def zscore_fit(x: np.ndarray, w: np.ndarray) -> tuple[float, float, float, float]:
    """Return the parameters that standardize one cross-section: the lower and upper
    winsor bounds (the median plus or minus 5 robust standard deviations), the w-weighted
    mean of the winsorized values and their equal-weighted standard deviation. A
    cross-section with fewer than 3 finite values, or with no spread, gets a standard
    deviation of 0, and zscore_apply then gives 0 for every value."""
    x = np.asarray(x, dtype=float)
    ok = np.isfinite(x)
    if ok.sum() < 3:
        return -np.inf, np.inf, 0.0, 0.0
    v = x[ok]
    med = np.median(v)
    mad = 1.4826 * np.median(np.abs(v - med))
    lo, hi = (med - 5 * mad, med + 5 * mad) if mad > 0 else (-np.inf, np.inf)
    v = np.clip(v, lo, hi)
    ww = w[ok]
    mu = np.average(v, weights=ww) if ww.sum() > 0 else v.mean()
    return float(lo), float(hi), float(mu), float(v.std())


def zscore_apply(x: np.ndarray, params: tuple[float, float, float, float],
                 clip: float = 3.0) -> np.ndarray:
    """Standardize x with the parameters of zscore_fit: winsorize, center, scale and clip at
    plus or minus clip. A missing value becomes 0, the mean. The values of x need not be
    the values that gave the parameters."""
    lo, hi, mu, sd = params
    x = np.asarray(x, dtype=float)
    z = np.zeros_like(x)
    ok = np.isfinite(x)
    if not sd > 0 or not ok.any():
        return z
    z[ok] = np.clip((np.clip(x[ok], lo, hi) - mu) / sd, -clip, clip)
    return z


def zscore(x: np.ndarray, w: np.ndarray, clip: float = 3.0) -> np.ndarray:
    """Standardize one cross-section. Winsorize at the median plus or minus 5 robust
    standard deviations, center on the w-weighted mean, divide by the equal-weighted
    standard deviation and clip at plus or minus clip. A missing value becomes 0, the
    mean. With w the market cap, the cap-weighted market has an exposure of 0."""
    return zscore_apply(x, zscore_fit(x, w), clip)


def standardize(day: np.ndarray, X: np.ndarray, w: np.ndarray,
                clip: float = 3.0) -> np.ndarray:
    """Return the z-score of each column of X within each session (zscore), centered on
    the w-weighted mean. Rows keep their order. A missing value becomes 0."""
    day = np.asarray(day)
    Z = np.zeros_like(X, dtype=float)
    order = np.argsort(day, kind="stable")
    _, starts = np.unique(day[order], return_index=True)
    ends = np.append(starts[1:], day.size)
    for a, b in zip(starts, ends, strict=True):
        rows = order[a:b]
        for j in range(X.shape[1]):
            Z[rows, j] = zscore(X[rows, j], w[rows], clip)
    return Z


def standardize_fit(day: np.ndarray, X: np.ndarray, w: np.ndarray
                    ) -> tuple[np.ndarray, np.ndarray]:
    """Return the sessions and the parameters of zscore_fit for each session and column of
    X, as an array of shape (sessions, columns, 4)."""
    day = np.asarray(day)
    order = np.argsort(day, kind="stable")
    days, starts = np.unique(day[order], return_index=True)
    ends = np.append(starts[1:], day.size)
    P = np.zeros((days.size, X.shape[1], 4))
    for i, (a, b) in enumerate(zip(starts, ends, strict=True)):
        rows = order[a:b]
        for j in range(X.shape[1]):
            P[i, j] = zscore_fit(X[rows, j], w[rows])
    return days, P


def standardize_apply(day: np.ndarray, X: np.ndarray, days: np.ndarray, P: np.ndarray,
                      clip: float = 3.0) -> tuple[np.ndarray, np.ndarray]:
    """Return the z-scores of the rows of X under the parameters of standardize_fit, and
    a flag for the rows whose session has parameters. The z-scores of a row with no
    parameters are 0. Rows keep their order."""
    day = np.asarray(day)
    Z = np.zeros_like(X, dtype=float)
    known = np.isin(day, days)
    order = np.argsort(day, kind="stable")
    sessions, starts = np.unique(day[order], return_index=True)
    ends = np.append(starts[1:], day.size)
    for d, a, b in zip(sessions, starts, ends, strict=True):
        i = np.searchsorted(days, d)
        if i >= days.size or days[i] != d:
            continue
        rows = order[a:b]
        for j in range(X.shape[1]):
            Z[rows, j] = zscore_apply(X[rows, j], tuple(P[i, j]), clip)
    return Z, known


def weighted_median(x: np.ndarray, w: np.ndarray) -> float:
    """Return the smallest value of x with at least half of the total weight w at or below
    it."""
    order = np.argsort(x, kind="stable")
    cum = np.cumsum(w[order])
    return float(x[order][np.searchsorted(cum, 0.5 * cum[-1])])


def clip_returns(r: np.ndarray, w: np.ndarray, k: float) -> np.ndarray:
    """Clip the returns r of one session at their w-weighted median plus or minus k robust
    standard deviations (1.4826 times the w-weighted median absolute deviation). Return r
    unchanged when k is 0 or the deviation is 0."""
    if k <= 0:
        return r
    med = weighted_median(r, w)
    mad = 1.4826 * weighted_median(np.abs(r - med), w)
    if not mad > 0:
        return r
    return np.clip(r, med - k * mad, med + k * mad)


def cross_section_returns(
    day: np.ndarray, Z: np.ndarray, r: np.ndarray, w: np.ndarray, *,
    groups: np.ndarray | None = None, n_groups: int = 0, cap: np.ndarray | None = None,
    min_names: int = 100, return_clip: float = 0.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Regress the returns of each session on the exposures Z, with weights w. Return the
    sessions, the market return, the style returns (one column for each column of Z), the
    group returns (one column for each group), the count of names and the weighted R².

    Without groups, the design has an intercept, and the intercept is the market. With
    groups (an integer code from 0 to n_groups - 1 for each row), the design has one dummy
    for each group and no intercept. The market is then the mean of the group
    coefficients weighted by cap, and each group return is its coefficient less the
    market, so the cap-weighted group returns sum to 0 (the constraint of a Barra model).
    A group with no names in a session gets NaN. A session with fewer than min_names
    usable rows gets NaN.

    With return_clip above 0, the fit uses the returns clipped by clip_returns, so one
    extreme return has a bounded effect on a coefficient. The residual and the R² use the
    raw returns. With 0, the fit uses the raw returns."""
    day = np.asarray(day)
    order = np.argsort(day, kind="stable")
    days, starts = np.unique(day[order], return_index=True)
    ends = np.append(starts[1:], day.size)
    k = Z.shape[1]
    market = np.full(days.size, np.nan)
    F = np.full((days.size, k), np.nan)
    G = np.full((days.size, n_groups), np.nan)
    n = np.zeros(days.size, dtype=int)
    r2 = np.full(days.size, np.nan)
    cap = w if cap is None else cap
    for i, (a, b) in enumerate(zip(starts, ends, strict=True)):
        rows = order[a:b]
        rr, ww, zz = r[rows], w[rows], Z[rows]
        m = np.isfinite(rr) & np.isfinite(ww) & (ww > 0)
        n[i] = int(m.sum())
        if n[i] < min_names:
            continue
        rr, ww, zz = rr[m], ww[m], zz[m]
        if groups is None:
            A = np.column_stack([np.ones(rr.size), zz])
        else:
            gg = groups[rows][m]
            present = np.unique(gg)
            D = (gg[:, None] == present[None, :]).astype(float)
            A = np.column_stack([D, zz])
        sw = np.sqrt(ww)
        coef, *_ = np.linalg.lstsq(
            A * sw[:, None], clip_returns(rr, ww, return_clip) * sw, rcond=None)
        if groups is None:
            market[i] = coef[0]
            F[i] = coef[1:]
        else:
            g = coef[:present.size]
            cc = cap[rows][m]
            share = np.array([cc[gg == c].sum() for c in present])
            total = share.sum()
            share = share / total if total > 0 else np.full(present.size, 1 / present.size)
            market[i] = share @ g
            G[i, present] = g - market[i]
            F[i] = coef[present.size:]
        resid = rr - A @ coef
        mean = np.average(rr, weights=ww)
        ss_tot = np.sum(ww * (rr - mean) ** 2)
        r2[i] = 1 - np.sum(ww * resid ** 2) / ss_tot if ss_tot > 0 else np.nan
    return days, market, F, G, n, r2


def style_returns(
    day: np.ndarray, X: np.ndarray, r: np.ndarray, w: np.ndarray, *, min_names: int = 100
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Standardize the characteristics X within each session and regress the returns on
    them with an intercept. Return the sessions, the factor returns (the intercept first,
    then one column for each column of X), the count of names and the weighted R²."""
    Z = standardize(day, X, w)
    days, market, F, _, n, r2 = cross_section_returns(day, Z, r, w, min_names=min_names)
    return days, np.column_stack([market, F]), n, r2


def pca_returns(
    R: np.ndarray, U: np.ndarray, A: np.ndarray, *,
    window: int = 252, step: int = 21, k: int = 5, n_names: int = 1000,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return the daily returns of the top k eigenportfolios, the variance share of each
    component at its rebalance and the count of names.

    At each rebalance row d, the eligible names (sdp.risk.eligible) give a correlation
    matrix over rows d - window to d - 1. The weights of component j are its eigenvector
    divided by the volatility of each name, scaled to a gross exposure of 1. The weights
    earn the returns of rows d to d + step - 1, so each return is out of sample. A name
    with no return on a row earns 0. The first component has a positive net weight. The
    sign of each later component follows the previous rebalance, so a series does not
    flip. The order of components 2 to k can change between rebalances."""
    T, N = R.shape
    F = np.full((T, k), np.nan)
    share = np.full((T, k), np.nan)
    used = np.zeros(T, dtype=int)
    prev: np.ndarray | None = None
    for d in range(window, T, step):
        cols = eligible(R, U, A, d, window, n_names)
        if cols.size < 2 * k:
            continue
        X = R[d - window:d, cols]
        sd = X.std(axis=0, ddof=1)
        live = sd > 0
        cols, X, sd = cols[live], X[:, live], sd[live]
        Z = (X - X.mean(axis=0)) / sd
        _, s, Vt = np.linalg.svd(Z, full_matrices=False)
        ev = s ** 2
        W = np.zeros((N, k))
        W[cols] = Vt[:k].T / sd[:, None]
        W /= np.abs(W).sum(axis=0)
        for j in range(k):
            flip = W[:, j].sum() < 0 if prev is None else W[:, j] @ prev[:, j] < 0
            if flip:
                W[:, j] *= -1
        prev = W
        end = min(d + step, T)
        F[d:end] = np.nan_to_num(R[d:end]) @ W
        share[d:end] = ev[:k] / ev.sum()
        used[d:end] = cols.size
    return F, share, used


def mimic_returns(
    y: np.ndarray, B: np.ndarray, *,
    window: int = 252, step: int = 21, min_obs: int = 126, ridge: float = 1e-4,
) -> tuple[np.ndarray, np.ndarray]:
    """Return an out-of-sample tracking portfolio for the target y and the R squared of
    each fit.

    At each rebalance row d, a ridge regression of y on the base returns B over rows
    d - window to d - 1 gives the weights b. Rows with a missing target or base value do
    not enter the fit. The portfolio B @ b earns rows d to d + step - 1. It has no
    intercept, because an intercept is not a traded asset. The target can end before B
    (a published factor lags), and the portfolio then carries it forward."""
    T, m = B.shape
    out = np.full(T, np.nan)
    r2 = np.full(T, np.nan)
    for d in range(window, T, step):
        yy, BB = y[d - window:d], B[d - window:d]
        ok = np.isfinite(yy) & np.isfinite(BB).all(axis=1)
        if ok.sum() < min_obs:
            continue
        Xf = np.column_stack([np.ones(ok.sum()), BB[ok]])
        # The penalty scales with the base returns, and the intercept has none.
        pen = ridge * np.sum(BB[ok] ** 2) / m * np.eye(m + 1)
        pen[0, 0] = 0.0
        coef = np.linalg.solve(Xf.T @ Xf + pen, Xf.T @ yy[ok])
        fit = Xf @ coef
        ss_tot = np.sum((yy[ok] - yy[ok].mean()) ** 2)
        end = min(d + step, T)
        Bn = B[d:end]
        out[d:end] = np.where(np.isfinite(Bn).all(axis=1), Bn @ coef[1:], np.nan)
        r2[d:end] = 1 - np.sum((yy[ok] - fit) ** 2) / ss_tot if ss_tot > 0 else np.nan
    return out, r2


def ridge_loadings(
    y: np.ndarray, X: np.ndarray, penalty: float
) -> tuple[np.ndarray, np.ndarray]:
    """Return the loadings and the intercept of a ridge regression of each column of the
    n x m matrix y on the n x k matrix X. The penalty acts on the standardized columns of X.
    The intercept has no penalty. The loadings are in the units of X. A column of X with no
    variance gets the loading 0."""
    mu = X.mean(axis=0)
    sd = np.where(np.ptp(X, axis=0) > 0, X.std(axis=0), np.inf)
    Z = (X - mu) / sd
    ym = y.mean(axis=0)
    coef = np.linalg.solve(Z.T @ Z + penalty * np.eye(X.shape[1]), Z.T @ (y - ym))
    beta = coef / sd[:, None]
    return beta, ym - mu @ beta


def fund_loadings(
    R: np.ndarray, X: np.ndarray, U: np.ndarray, *,
    window: int = 252, step: int = 21, min_obs: int = 126, min_returns: int = 252,
    penalty: float = 5.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Return the rebalance rows, the loadings (fits x funds x factors), the intercepts,
    the in-sample R squared and the count of rows of each fit.

    R holds the daily returns of the funds and X the factor returns, one row for each
    session. At each rebalance row d, a ridge regression (ridge_loadings) of the return of
    a fund on X over rows d - window to d - 1 gives the loadings. The loadings earn the rows
    d to d + step - 1. A fund gets a fit when U is true for it on row d and it has at least
    min_returns returns before row d. At least min_obs rows of the window must also have
    its return and every factor. Other funds get NaN. The fit reads no row from d on."""
    T, N = R.shape
    K = X.shape[1]
    seen = np.cumsum(np.isfinite(R), axis=0)
    rows = np.arange(window, T, step)
    B = np.full((rows.size, N, K), np.nan)
    alpha = np.full((rows.size, N), np.nan)
    r2 = np.full((rows.size, N), np.nan)
    n_obs = np.zeros((rows.size, N), dtype=int)
    for i, d in enumerate(rows):
        Xw, Rw = X[d - window:d], R[d - window:d]
        fin = np.isfinite(Rw) & np.isfinite(Xw).all(axis=1)[:, None]
        n_obs[i] = fin.sum(axis=0)
        ok = U[d] & (n_obs[i] >= min_obs) & (seen[d - 1] >= min_returns)
        cols = np.flatnonzero(ok)
        if cols.size == 0:
            continue
        # Funds with the same usable rows share one design, so they share one solve.
        _, group = np.unique(np.packbits(fin[:, cols], axis=0).T, axis=0, return_inverse=True)
        group = group.reshape(-1)
        for g in np.unique(group):
            c = cols[group == g]
            use = fin[:, c[0]]
            y = Rw[use][:, c]
            b, a = ridge_loadings(y, Xw[use], penalty)
            B[i, c] = b.T
            alpha[i, c] = a
            ss_tot = ((y - y.mean(axis=0)) ** 2).sum(axis=0)
            ss_res = ((y - a - Xw[use] @ b) ** 2).sum(axis=0)
            r2[i, c] = np.where(ss_tot > 0, 1 - ss_res / np.where(ss_tot > 0, ss_tot, 1), np.nan)
    return rows, B, alpha, r2, n_obs

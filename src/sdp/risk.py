# src/sdp/risk.py
"""Covariance estimators and risk models for a T x N return matrix. NumPy only,
with no I/O. Each estimator takes one row per session and one column per name.
"""
from __future__ import annotations

import numpy as np


def gmv(S: np.ndarray) -> np.ndarray:
    """Return the global minimum-variance weights w = S^-1 1 / (1' S^-1 1). A small ridge
    replaces the solve when S is exactly singular."""
    n = S.shape[0]
    one = np.ones(n)
    try:
        x = np.linalg.solve(S, one)
    except np.linalg.LinAlgError:
        x = np.linalg.solve(S + 1e-8 * np.trace(S) / n * np.eye(n), one)
    return x / (one @ x)


def sample_cov(X: np.ndarray) -> np.ndarray:
    """Return the sample covariance with the T - 1 divisor."""
    return np.cov(X, rowvar=False)


def pca_cov(X: np.ndarray) -> np.ndarray:
    """Return a statistical factor model: the top K eigen-directions of the sample
    covariance plus a diagonal idiosyncratic part. K is the count of correlation
    eigenvalues above the Marchenko-Pastur edge, with a minimum of 1."""
    S = np.cov(X, rowvar=False)
    v = np.linalg.eigvalsh(np.corrcoef(X, rowvar=False))
    q = X.shape[1] / X.shape[0]
    lam_plus = (1 + np.sqrt(q)) ** 2
    K = max(1, int((v > lam_plus).sum()))
    vs, Vs = np.linalg.eigh(S)
    idx = np.argsort(vs)[::-1][:K]
    low_rank = Vs[:, idx] @ np.diag(vs[idx]) @ Vs[:, idx].T
    idio = np.clip(np.diag(S) - np.diag(low_rank), 1e-12, None)
    return low_rank + np.diag(idio)


def rmt_cov(X: np.ndarray) -> np.ndarray:
    """Return the random-matrix clipped covariance. The correlation eigenvalues inside
    the Marchenko-Pastur band get their mean, and the sample volatilities rescale the
    cleaned correlation."""
    S = np.cov(X, rowvar=False)
    C = np.corrcoef(X, rowvar=False)
    d = np.sqrt(np.diag(S))
    v, V = np.linalg.eigh(C)
    q = X.shape[1] / X.shape[0]
    lam_plus = (1 + np.sqrt(q)) ** 2
    keep = v > lam_plus
    v_clean = np.where(keep, v, v[~keep].mean() if (~keep).any() else 0.0)
    v_clean *= C.shape[0] / v_clean.sum()
    Cc = V @ np.diag(v_clean) @ V.T
    dd = np.sqrt(np.diag(Cc))
    Cc = Cc / np.outer(dd, dd)
    return np.outer(d, d) * Cc


def qis(
    X: np.ndarray, return_spectrum: bool = False
) -> np.ndarray | tuple[np.ndarray, np.ndarray]:
    """Return the Ledoit-Wolf quadratic-inverse shrinkage (QIS) covariance, a port of the
    authors' reference code. It keeps the sample eigenvectors and gives each eigenvalue
    its own correction, for T > N and for N > T. With return_spectrum it returns the
    ascending sample eigenvalues and their shrunk values."""
    T, N = X.shape
    Xc = X - X.mean(0)
    n = T - 1
    c = N / n
    S = (Xc.T @ Xc) / n
    S = (S + S.T) / 2
    lam, u = np.linalg.eigh(S)
    lam = np.clip(lam, 0.0, None)
    h = (min(c ** 2, 1 / c ** 2) ** 0.35) / N ** 0.35
    inv = 1.0 / lam[max(1, N - n + 1) - 1:N]
    m = inv.size
    Lj = np.tile(inv, (m, 1)).T
    Lji = Lj - Lj.T
    denom = Lji ** 2 + (Lj ** 2) * h ** 2
    theta = np.mean(Lj * Lji / denom, axis=0)
    htheta = np.mean(Lj * Lj * h / denom, axis=0)
    atheta2 = theta ** 2 + htheta ** 2
    if n >= N:
        delta = 1.0 / ((1 - c) ** 2 * inv + 2 * c * (1 - c) * inv * theta
                       + c ** 2 * inv * atheta2)
    else:
        delta0 = 1.0 / ((c - 1) * np.mean(inv))
        delta = np.concatenate([np.repeat(delta0, N - n), 1.0 / (inv * atheta2)])
    delta = delta * (lam.sum() / delta.sum())
    if return_spectrum:
        return lam, delta
    return (u * delta) @ u.T


def ff_cov(
    R: np.ndarray, F: np.ndarray, rf: np.ndarray | None = None
) -> tuple[np.ndarray, np.ndarray]:
    """Return (Sigma, B) for a time-series factor model, Sigma = B Omega B' + diag(resid
    var). Each name regresses on an intercept and the T x K factor returns F, and on
    excess returns R - rf when rf is given."""
    Y = R if rf is None else R - np.asarray(rf)[:, None]
    F1 = np.column_stack([np.ones(len(F)), F])
    coef, *_ = np.linalg.lstsq(F1, Y, rcond=None)
    B = coef[1:].T
    resid = Y - F1 @ coef
    rv = (resid ** 2).sum(0) / (len(F) - F1.shape[1])
    omega = np.atleast_2d(np.cov(F, rowvar=False))
    return B @ omega @ B.T + np.diag(rv), B

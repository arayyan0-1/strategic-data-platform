"""Active mean-variance under a tracking-error budget, in cvxpy.

In annual units, with alpha a forecast of each fund's return over the benchmark before fees:

    max   (alpha - fee)'w - risk_aversion * TE(w)^2 - (252 / horizon) * sum_i c_i |w_i - w0_i|
    s.t.  TE(w) <= te_max,  1'w = 1,  lower <= w <= upper,  sum of cash weights <= cash_max,

where TE(w)^2 = 252 w' S_active w. With a forecast of information ratio IR, the unconstrained
optimum takes a tracking error of about IR / (2 * risk_aversion) (Grinold and Kahn).
"""
from __future__ import annotations

from dataclasses import dataclass

import cvxpy as cp
import numpy as np

from sdp.fund.objectives import ACTIVE_VAR_FLOOR, SESSIONS_A_YEAR, Problem
from sdp.risk import sample_cov

RESAMPLES = 24


@dataclass(frozen=True)
class Budget:
    te_max: float = 0.03
    risk_aversion: float = 5.0
    cash_max: float = 0.30
    horizon: int = 63


@dataclass
class Solution:
    weights: np.ndarray
    status: str
    te: float
    te_capped: bool


def _factor(S: np.ndarray) -> np.ndarray:
    """Return L with L L' = S, for a symmetric positive semidefinite S."""
    S = (S + S.T) / 2
    vals, vecs = np.linalg.eigh(S)
    return vecs * np.sqrt(np.clip(vals, 0.0, None))


def solve(p: Problem, alpha: np.ndarray, cash: np.ndarray, budget: Budget,
          S_active: np.ndarray | None = None) -> Solution:
    """Solve the problem above. alpha is a year, per fund; cash marks the cash funds.

    When the bounds leave no portfolio within te_max (a locked position can force it), the cap
    is dropped and the risk aversion alone holds the tracking error; te_capped is then False.
    """
    S = p.S_active if S_active is None else S_active
    L = _factor(S)
    n = len(p.names)
    w = cp.Variable(n)
    mu = np.asarray(alpha, dtype=float) - p.fee
    trade = p.cost * SESSIONS_A_YEAR / budget.horizon
    te2 = SESSIONS_A_YEAR * cp.sum_squares(L.T @ w)
    objective = cp.Maximize(mu @ w - budget.risk_aversion * te2
                            - trade @ cp.abs(w - p.w0))
    base = [cp.sum(w) == 1, w >= p.lower, w <= p.upper]
    if cash.any():
        base.append(cp.sum(w[np.flatnonzero(cash)]) <= max(budget.cash_max,
                                                            float(p.lower[cash].sum())))
    capped = base + [np.sqrt(SESSIONS_A_YEAR) * cp.norm(L.T @ w, 2) <= budget.te_max]
    prob = cp.Problem(objective, capped)
    prob.solve(solver=cp.CLARABEL)
    te_capped = True
    if w.value is None or prob.status not in ("optimal", "optimal_inaccurate"):
        prob = cp.Problem(objective, base)
        prob.solve(solver=cp.CLARABEL)
        te_capped = False
    if w.value is None:
        raise RuntimeError(f"The tracking-error problem has no solution: {prob.status}.")
    x = np.asarray(w.value, dtype=float)
    te = float(np.sqrt(max(x @ S @ x, 0.0) * SESSIONS_A_YEAR))
    return Solution(weights=x, status=prob.status, te=te, te_capped=te_capped)


def solve_resampled(p: Problem, alpha: np.ndarray, cash: np.ndarray, budget: Budget,
                    seed: int = 0) -> Solution:
    """The mean of solve over RESAMPLES bootstrap draws of the active returns (Michaud).

    Each draw has its own sample covariance; alpha is not resampled, because it is a forecast,
    not a sample mean. The mean of feasible weights is feasible.
    """
    rng = np.random.default_rng(seed)
    A = p.X - p.y[:, None]
    ws = []
    for _ in range(RESAMPLES):
        draw = A[rng.integers(0, len(A), len(A))]
        S = sample_cov(draw) + ACTIVE_VAR_FLOOR * np.eye(A.shape[1])
        ws.append(solve(p, alpha, cash, budget, S_active=S).weights)
    x = np.mean(ws, axis=0)
    te = float(np.sqrt(max(x @ p.S_active @ x, 0.0) * SESSIONS_A_YEAR))
    return Solution(weights=x, status="resampled", te=te, te_capped=te <= budget.te_max + 1e-6)

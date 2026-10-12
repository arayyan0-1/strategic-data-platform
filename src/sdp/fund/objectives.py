"""The objectives the fund can rebalance to. Each maps a Problem to target weights.

Units are daily: returns, covariances and the expected return. A trade's cost is spread over
`amortize` sessions so it is comparable to a daily return. The relative objectives work on the
return less the benchmark's. The absolute ones ignore the benchmark.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
from skfolio.measures import RiskMeasure
from skfolio.moments import BaseCovariance, BaseMu
from skfolio.optimization import (
    DistributionallyRobustCVaR,
    HierarchicalRiskParity,
    MaximumDiversification,
    MeanRisk,
    ObjectiveFunction,
    RiskBudgeting,
)
from skfolio.prior import EmpiricalPrior
from skfolio.uncertainty_set import EmpiricalMuUncertaintySet

from sdp.risk import sample_cov

SESSIONS_A_YEAR = 252
ABSOLUTE_RISK_AVERSION = 3.0
RESAMPLES = 24
UNCERTAINTY_CONFIDENCE = 0.9
WASSERSTEIN_RADIUS = 0.001
# Least active variance a fund is given, daily: a tracking error of 1 bp a year. The benchmark
# fund itself has none, which the solvers refuse.
ACTIVE_VAR_FLOOR = 1e-4 ** 2 / SESSIONS_A_YEAR


@dataclass
class Problem:
    """One rebalance: the funds that may be held and what is known at the decision close.

    X and y are the trailing daily returns of the funds and of the benchmark, NaN as 0. S is the
    funds' daily covariance and S_active that of each fund's return less the benchmark's. fee and
    alpha are a year; alpha is a forecast beyond fees, 0 until a study supplies one. cost is the
    one-way cost of a trade as a fraction of its value. w0 is the current weights, and lower and
    upper bound the targets (lower is above 0 where the holding rule locks a fund).
    """

    names: list[str]
    X: np.ndarray
    y: np.ndarray
    S: np.ndarray
    S_active: np.ndarray
    fee: np.ndarray
    alpha: np.ndarray
    cost: np.ndarray
    w0: np.ndarray
    lower: np.ndarray
    upper: np.ndarray
    benchmark: str
    risk_aversion: float = 100.0
    amortize: int = 63

    @property
    def mu_active(self) -> np.ndarray:
        return (self.alpha - self.fee) / SESSIONS_A_YEAR


class _FixedMu(BaseMu):
    def __init__(self, mu=None):
        self.mu = mu

    def fit(self, X, y=None):
        self.mu_ = np.asarray(self.mu, dtype=float)
        return self


class _FixedCovariance(BaseCovariance):
    def __init__(self, matrix=None, nearest=True, higham=False, higham_max_iteration=100):
        super().__init__(nearest=nearest, higham=higham,
                         higham_max_iteration=higham_max_iteration)
        self.matrix = matrix

    def fit(self, X, y=None):
        self._set_covariance(np.asarray(self.matrix, dtype=float))
        return self


def _prior(mu: np.ndarray, S: np.ndarray) -> EmpiricalPrior:
    return EmpiricalPrior(mu_estimator=_FixedMu(mu), covariance_estimator=_FixedCovariance(S))


def _bounds(p: Problem) -> dict:
    return {"min_weights": p.lower, "max_weights": p.upper,
            "transaction_costs": p.cost / p.amortize, "previous_weights": p.w0}


def _utility(p: Problem, mu: np.ndarray, S: np.ndarray, X: np.ndarray, aversion: float,
             **kw) -> np.ndarray:
    m = MeanRisk(objective_function=ObjectiveFunction.MAXIMIZE_UTILITY,
                 risk_measure=RiskMeasure.VARIANCE, risk_aversion=aversion,
                 prior_estimator=_prior(mu, S), budget=1.0, **_bounds(p), **kw)
    return m.fit(X).weights_


def project(w: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> np.ndarray:
    """Return the nearest weights to w (Euclidean) within the bounds that sum to 1, or to the
    sum of the upper bounds when that is below 1."""
    if upper.sum() <= 1:
        return upper.copy()
    lo, hi = float(np.min(w - upper)), float(np.max(w - lower))
    for _ in range(100):
        tau = (lo + hi) / 2
        if np.clip(w - tau, lower, upper).sum() > 1:
            lo = tau
        else:
            hi = tau
    return np.clip(w - (lo + hi) / 2, lower, upper)


def benchmark(p: Problem) -> np.ndarray:
    """All in the benchmark fund."""
    w = np.zeros(len(p.names))
    w[p.names.index(p.benchmark)] = 1.0
    return w


def min_te(p: Problem) -> np.ndarray:
    """Least tracking error to the benchmark, after costs. Fees are ignored."""
    return _utility(p, np.zeros(len(p.names)), p.S_active, p.X - p.y[:, None], p.risk_aversion)


def mvo_te(p: Problem) -> np.ndarray:
    """Mean-variance on the return less the benchmark: alpha less fees, less risk aversion times
    the tracking variance, less costs."""
    return _utility(p, p.mu_active, p.S_active, p.X - p.y[:, None], p.risk_aversion)


def robust_te(p: Problem) -> np.ndarray:
    """mvo_te against the worst expected return in an ellipsoid around it at
    UNCERTAINTY_CONFIDENCE, scaled by the window's active covariance over its length."""
    prior = _prior(p.mu_active, p.S_active)
    return _utility(
        p, p.mu_active, p.S_active, p.X - p.y[:, None], p.risk_aversion,
        mu_uncertainty_set_estimator=EmpiricalMuUncertaintySet(
            prior_estimator=prior, confidence_level=UNCERTAINTY_CONFIDENCE))


def resampled_te(p: Problem) -> np.ndarray:
    """The mean of mvo_te over RESAMPLES bootstrap draws of the window (Michaud), each with the
    sample covariance of its draw."""
    rng = np.random.default_rng(0)
    A = p.X - p.y[:, None]
    ws = []
    for _ in range(RESAMPLES):
        draw = A[rng.integers(0, len(A), len(A))]
        S = sample_cov(draw) + ACTIVE_VAR_FLOOR * np.eye(A.shape[1])
        ws.append(_utility(p, p.mu_active, S, draw, p.risk_aversion))
    return np.mean(ws, axis=0)


def equal_weight(p: Problem) -> np.ndarray:
    """1/N."""
    return np.full(len(p.names), 1 / len(p.names))


def inverse_vol(p: Problem) -> np.ndarray:
    """Weights in proportion to one over each fund's volatility."""
    w = 1 / np.sqrt(np.diag(p.S))
    return w / w.sum()


def min_variance(p: Problem) -> np.ndarray:
    """Least variance, after costs."""
    return _utility(p, np.zeros(len(p.names)), p.S, p.X, ABSOLUTE_RISK_AVERSION)


def mvo_sample(p: Problem) -> np.ndarray:
    """Mean-variance with the window's mean returns as the forecast: the textbook form."""
    return _utility(p, p.X.mean(axis=0), p.S, p.X, ABSOLUTE_RISK_AVERSION)


def risk_parity(p: Problem) -> np.ndarray:
    """Each fund adds the same share of the variance."""
    m = RiskBudgeting(risk_measure=RiskMeasure.VARIANCE,
                      prior_estimator=_prior(np.zeros(len(p.names)), p.S), **_bounds(p))
    return m.fit(p.X).weights_


def hrp(p: Problem) -> np.ndarray:
    """Hierarchical risk parity (Lopez de Prado): split the variance down a tree of the
    correlations, with no matrix inverse."""
    m = HierarchicalRiskParity(prior_estimator=_prior(np.zeros(len(p.names)), p.S),
                               min_weights=p.lower, max_weights=p.upper)
    return m.fit(p.X).weights_


def max_diversification(p: Problem) -> np.ndarray:
    """The largest ratio of the weighted mean volatility to the portfolio volatility."""
    m = MaximumDiversification(prior_estimator=_prior(np.zeros(len(p.names)), p.S),
                               budget=1.0, **_bounds(p))
    return m.fit(p.X).weights_


def min_cvar(p: Problem) -> np.ndarray:
    """Least mean loss on the worst 5% of the window's days."""
    m = MeanRisk(objective_function=ObjectiveFunction.MINIMIZE_RISK,
                 risk_measure=RiskMeasure.CVAR, budget=1.0, **_bounds(p))
    return m.fit(p.X).weights_


def robust_cvar(p: Problem) -> np.ndarray:
    """Least CVaR against every distribution within a Wasserstein ball of the window's days."""
    m = DistributionallyRobustCVaR(risk_aversion=1.0,
                                   wasserstein_ball_radius=WASSERSTEIN_RADIUS,
                                   prior_estimator=_prior(np.zeros(len(p.names)), p.S),
                                   min_weights=p.lower, max_weights=p.upper,
                                   previous_weights=p.w0, budget=1.0)
    return m.fit(p.X).weights_


STRATEGIES: dict[str, tuple[str, Callable[[Problem], np.ndarray]]] = {
    "benchmark": ("relative", benchmark),
    "min_te": ("relative", min_te),
    "mvo_te": ("relative", mvo_te),
    "robust_te": ("relative", robust_te),
    "resampled_te": ("relative", resampled_te),
    "equal_weight": ("absolute", equal_weight),
    "inverse_vol": ("absolute", inverse_vol),
    "min_variance": ("absolute", min_variance),
    "mvo_sample": ("absolute", mvo_sample),
    "risk_parity": ("absolute", risk_parity),
    "hrp": ("absolute", hrp),
    "max_diversification": ("absolute", max_diversification),
    "min_cvar": ("absolute", min_cvar),
    "robust_cvar": ("absolute", robust_cvar),
}

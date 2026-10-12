"""Daily covariance estimates of fund returns, and of each fund's return less a benchmark's."""
from __future__ import annotations

import numpy as np

from sdp.risk import ew_cov, qis, sample_cov

METHODS = ("sample", "ewma", "qis", "factor")
EWMA_HALF_LIFE = 63


def estimate(X: np.ndarray, method: str) -> np.ndarray:
    """Return the covariance of the T x N returns X by a statistical method."""
    if method == "sample":
        return sample_cov(X)
    if method == "ewma":
        return ew_cov(X, EWMA_HALF_LIFE)
    if method == "qis":
        return qis(X)
    raise ValueError(f"method must be one of {METHODS[:-1]} here, not {method!r}.")


def active(S: np.ndarray, b: int) -> np.ndarray:
    """Return the covariance of r_i - r_b for every column i, where b is the benchmark's."""
    c = S[:, b]
    return S - c[:, None] - c[None, :] + S[b, b]

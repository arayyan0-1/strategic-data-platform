"""Covariance estimators and risk models in sdp.risk.

Each test uses a small synthetic panel with a fixed seed. The two shapes cover
T > N and N > T, because QIS takes a different path for each.
"""
import numpy as np
import pytest

from sdp import risk

REGIMES = [(7, 60, 40), (11, 40, 60)]  # (seed, T, N)

# Sorted eigenvalues of qis at ranks 0, 1, N//2, N-2 and N-1, computed once.
GOLDEN = {
    7: [0.00013444271642973853, 0.00020559082485389633, 0.00042589872761421635,
        0.0020739624884999848, 0.0020744249048109415],
    11: [0.0002794120441289372, 0.0003039465135462962, 0.0004011413834458421,
         0.003514999450459446, 0.004312240231547218],
}


def returns(seed: int, T: int, N: int, k: int = 2) -> np.ndarray:
    """Return a T x N panel with k common factors and idiosyncratic noise."""
    rng = np.random.default_rng(seed)
    f = rng.standard_normal((T, k))
    b = rng.standard_normal((N, k))
    return 0.01 * (f @ b.T + 2.0 * rng.standard_normal((T, N)))


def assert_spd(S: np.ndarray) -> None:
    np.testing.assert_allclose(S, S.T, rtol=0, atol=1e-15)
    assert np.linalg.eigvalsh(S).min() > 0


@pytest.mark.parametrize("seed,T,N", REGIMES)
def test_qis_is_symmetric_positive_definite(seed, T, N):
    assert_spd(risk.qis(returns(seed, T, N)))


@pytest.mark.parametrize("seed,T,N", REGIMES)
def test_qis_preserves_the_trace_of_the_sample_covariance(seed, T, N):
    X = returns(seed, T, N)
    np.testing.assert_allclose(np.trace(risk.qis(X)), np.trace(risk.sample_cov(X)),
                               rtol=1e-10)


@pytest.mark.parametrize("seed,T,N", REGIMES)
def test_qis_keeps_the_sample_eigenvectors(seed, T, N):
    # Two symmetric matrices commute when they have the same eigenvectors.
    X = returns(seed, T, N)
    S, Q = risk.sample_cov(X), risk.qis(X)
    scale = np.linalg.norm(S) * np.linalg.norm(Q)
    np.testing.assert_allclose(S @ Q, Q @ S, rtol=0, atol=1e-12 * scale)


@pytest.mark.parametrize("seed,T,N", REGIMES)
def test_qis_spectrum_matches_the_golden_values(seed, T, N):
    ev = np.linalg.eigvalsh(risk.qis(returns(seed, T, N)))
    np.testing.assert_allclose(ev[[0, 1, N // 2, N - 2, N - 1]], GOLDEN[seed], rtol=1e-10)


@pytest.mark.parametrize("seed,T,N", REGIMES)
def test_qis_spectrum_is_the_spectrum_of_the_matrix(seed, T, N):
    X = returns(seed, T, N)
    lam, delta = risk.qis(X, return_spectrum=True)
    assert lam.shape == delta.shape == (N,)
    assert np.all(np.diff(lam) >= 0)
    np.testing.assert_allclose(np.sort(delta), np.linalg.eigvalsh(risk.qis(X)), rtol=1e-10)


@pytest.mark.parametrize("seed,T,N", REGIMES)
@pytest.mark.parametrize("estimator", [risk.pca_cov, risk.rmt_cov])
def test_factor_and_clipped_estimators_are_positive_definite(estimator, seed, T, N):
    assert_spd(estimator(returns(seed, T, N)))


@pytest.mark.parametrize("seed,T,N", REGIMES)
def test_gmv_weights_sum_to_one_and_beat_equal_weight(seed, T, N):
    S = risk.qis(returns(seed, T, N))
    w = risk.gmv(S)
    ew = np.full(N, 1 / N)
    assert w.sum() == pytest.approx(1.0, abs=1e-12)
    assert w @ S @ w <= ew @ S @ ew


def test_gmv_weights_sum_to_one_on_a_singular_matrix():
    S = np.ones((3, 3))
    assert risk.gmv(S).sum() == pytest.approx(1.0, abs=1e-12)


def factor_panel(T=500, N=20, K=3, noise=1e-4, seed=3):
    """Return (R, F, rf, B) where R = rf + alpha + F B' + small noise."""
    rng = np.random.default_rng(seed)
    F = 0.01 * rng.standard_normal((T, K))
    B = rng.uniform(-1.5, 1.5, (N, K))
    alpha = 1e-4 * rng.standard_normal(N)
    rf = 1e-4 + 1e-3 * rng.standard_normal(T)
    R = rf[:, None] + alpha + F @ B.T + noise * rng.standard_normal((T, N))
    return R, F, rf, B


def test_ff_cov_shapes():
    R, F, rf, _ = factor_panel()
    Sigma, B = risk.ff_cov(R, F, rf)
    assert Sigma.shape == (R.shape[1], R.shape[1])
    assert B.shape == (R.shape[1], F.shape[1])
    assert_spd(Sigma)


def test_ff_cov_recovers_known_loadings():
    R, F, rf, B_true = factor_panel()
    _, B = risk.ff_cov(R, F, rf)
    np.testing.assert_allclose(B, B_true, rtol=0, atol=1e-3)


def test_ff_cov_regresses_excess_returns_when_rf_is_given():
    R, F, rf, _ = factor_panel()
    Sigma, B = risk.ff_cov(R, F, rf)
    Sigma_x, B_x = risk.ff_cov(R - rf[:, None], F)
    np.testing.assert_allclose(Sigma, Sigma_x, rtol=1e-12)
    np.testing.assert_allclose(B, B_x, rtol=1e-12)
    # Without rf, the variation of rf goes into the residual variance.
    Sigma_raw, _ = risk.ff_cov(R, F)
    assert np.diag(Sigma_raw).mean() > np.diag(Sigma).mean()


def test_ff_cov_accepts_one_factor():
    R, F, rf, _ = factor_panel(K=1)
    Sigma, B = risk.ff_cov(R, F, rf)
    assert Sigma.shape == (R.shape[1], R.shape[1])
    assert B.shape == (R.shape[1], 1)

"""Tests for the set-and-forget weights: the fill, the assumptions, the paths, the three ways of
holding and the solver."""
import datetime as dt

import numpy as np
import pytest

from sdp.fund import allocation as al


def _history(T=240, seed=0, missing=None):
    rng = np.random.default_rng(seed)
    a = rng.normal(0.008, 0.04, T)
    b = rng.normal(0.004, 0.02, T)
    c = 0.5 * a + 0.3 * b + rng.normal(0.001, 0.01, T)
    cash = np.full(T, 0.003)
    ret = np.column_stack([a, b, c, cash])
    if missing:
        ret[:missing, 2] = np.nan
    months = [dt.date(2000 + i // 12, i % 12 + 1, 1) for i in range(T)]
    return al.History(names=["a", "b", "c", "cash"], kinds=["stock", "bond", "stock", "cash"],
                      months=months, ret=ret, rf=cash.copy(), gap=np.zeros(4))


def _R(S=40, T=120, N=3, seed=1):
    rng = np.random.default_rng(seed)
    return rng.normal([0.006, 0.003, 0.0], [0.05, 0.02, 0.001], size=(S, T, N))


def test_the_fill_keeps_observed_months_and_fits_the_missing_ones():
    h = _history(missing=60)
    f = al.fit_fill(h, ["a", "b"])
    assert np.array_equal(f.filled[60:], h.ret[60:]), "observed months are kept"
    assert not np.isnan(f.filled).any()
    b = f.beta[2]
    assert b[1] == pytest.approx(0.5, abs=0.05) and b[2] == pytest.approx(0.3, abs=0.1)
    (cols, pool), = f.pools
    assert list(cols) == [2] and pool[0] == 60 and len(pool) == 180
    assert f.resid[pool, 2].mean() == pytest.approx(0, abs=1e-15), "residuals are centred"


def test_a_regressor_with_a_gap_is_refused():
    h = _history(missing=10)
    with pytest.raises(ValueError, match="regressor"):
        al.fit_fill(h, ["a", "c"])


def test_the_blend_moves_a_short_record_more_than_a_long_one():
    h = _history(T=600, missing=480)
    f = al.fit_fill(h, ["a", "b"])
    p = al.premia(h, f)
    assert p.history[3] == p.equal[3] == p.blend[3] == 0, "cash earns no premium"
    sr_hist, sr_blend = p.history[:3] / p.sigma[:3], p.blend[:3] / p.sigma[:3]
    pull = np.abs(sr_blend - p.s) / np.abs(sr_hist - p.s)
    assert pull[2] < pull[0], "ten years are pulled toward s more than fifty"
    assert np.allclose(p.equal[:3] / p.sigma[:3], p.s)


def test_rolling_vol_skips_windows_with_a_missing_month():
    x = np.tile([0.1, -0.1], 30).astype(float)
    x[5] = np.nan
    v = al.rolling_vol(x[:, None])
    full = al.rolling_vol(np.tile([0.1, -0.1], 30).astype(float)[:, None])
    assert np.isfinite(v[0]) and v[0] == pytest.approx(full[0], rel=0.3)


def test_a_path_with_no_jump_reads_the_months_in_order_and_shifts_the_mean():
    h = _history()
    f = al.fit_fill(h, ["a", "b"])
    rng = np.random.default_rng(3)
    p = al.bootstrap(h, f, n_paths=1, n_months=24, block=1e12, rng=rng)
    start = int(np.flatnonzero(np.isclose(h.ret[:, 0], p.gross[0, 0, 0]))[0])
    assert np.allclose(p.gross[0], h.ret[np.arange(start, start + 24) % 240])
    R = p.funds(np.array([0.06, 0.0, 0.0, 0.0]), np.array([-0.012, 0, 0, 0]))
    shift = 0.06 / 12 - p.base[0] - 0.001
    assert np.allclose(R[0, :, 0], p.gross[0, :, 0] + shift)


def test_missing_months_take_a_fit_plus_a_donor_residual():
    h = _history(missing=60)
    f = al.fit_fill(h, ["a", "b"])
    p = al.bootstrap(h, f, n_paths=200, n_months=36, block=24, rng=np.random.default_rng(4))
    early = np.isnan(h.ret[:, 2])
    assert np.std(p.gross[:, :, 2]) == pytest.approx(np.std(h.ret[~early, 2]), rel=0.2)


def test_yearly_matches_the_monthly_simulation_with_costs():
    R = _R()
    w = np.array([0.5, 0.3, 0.2])
    V = al.simulate(R, w, "yearly")
    y = al.Yearly(R)
    net, _ = y._net(w)
    assert np.allclose(V[:, 12::12], np.cumprod(net, axis=1))
    assert al.measures(V)["growth"] == pytest.approx(y.growth(w)[0])
    assert al.measures(V)["risk"] ** 2 == pytest.approx(y.risk(w)[0])


def test_never_is_the_weighted_sum_of_each_piece_held_alone():
    R = _R()
    w = np.array([0.5, 0.3, 0.2])
    V = al.simulate(R, w, "never")
    assert np.allclose(V[:, -1], np.prod(1 + R, axis=1) @ w)
    n = al.Never(R)
    assert al.measures(V)["growth"] == pytest.approx(n.growth(w)[0])
    assert al.measures(V)["risk"] ** 2 == pytest.approx(n.risk(w)[0])


def test_drift_matches_the_monthly_simulation():
    R = _R()
    w = np.array([0.5, 0.3, 0.2])
    V = al.simulate(R, w, "drift")
    d = al.Drift(R)
    assert d.growth(w)[0] == pytest.approx(al.measures(V)["growth"])
    assert d.risk(w)[0] == pytest.approx(al.measures(V)["risk"] ** 2)


@pytest.mark.parametrize("model", [al.Yearly, al.Never, al.Drift])
def test_the_gradients_match_finite_differences(model):
    m = model(_R())
    w = np.array([0.5, 0.3, 0.2])
    for f in (m.growth, m.risk):
        g = f(w)[1]
        for j in range(3):
            e = np.zeros(3)
            e[j] = 1e-6
            assert g[j] == pytest.approx((f(w + e)[0] - f(w - e)[0]) / 2e-6, rel=1e-4, abs=1e-7)


def test_drift_trades_only_when_a_piece_moves_past_its_limit():
    R = np.zeros((2, 24, 2))
    R[1, 5, 0] = 0.30
    w = np.array([0.6, 0.4])
    V = al.simulate(R, w, "drift")
    assert np.allclose(V[0], 1), "no move, no trade"
    v = 1.18
    turnover = abs(0.78 - v * 0.6) + abs(0.4 - v * 0.4)
    assert V[1, 6] == pytest.approx(v - al.TRADE_COST * turnover)
    assert V[1, -1] == pytest.approx(V[1, 6])


def test_drift_uses_the_smaller_limit_for_a_small_piece():
    R = np.zeros((1, 12, 2))
    R[0, 0, 1] = 0.30
    V = al.simulate(R, np.array([0.96, 0.04]), "drift")
    assert V[0, 1] < 1 + 0.04 * 0.30, "a 4% piece that grows 30% is past 25% of its target"


def test_the_solver_finds_the_mix_of_cash_and_a_risky_piece_at_a_risk_limit():
    rng = np.random.default_rng(5)
    S, T = 400, 240
    R = np.zeros((S, T, 2))
    R[:, :, 0] = rng.normal(0.008, 0.045, size=(S, T))
    R[:, :, 1] = 0.002
    m = al.Yearly(R, cost=0.0)
    risky = np.sqrt(m.risk(np.array([1.0, 0.0]))[0])
    w, ok = al.solve(m, 2, sigma=risky / 2)
    assert ok and w[0] == pytest.approx(0.5, abs=0.01)
    assert np.sqrt(m.risk(w)[0]) == pytest.approx(risky / 2, rel=1e-3)


def test_the_frontier_marks_limits_below_the_least_risk_as_infeasible():
    R = _R(S=60)
    pts = al.frontier(al.Yearly(R), 3, sigmas=(0.0001, 0.05, 0.10))
    assert pts[0]["limit"] == "min" and pts[1]["weights"] is None
    growth = [al.Yearly(R).growth(p["weights"])[0] for p in pts[2:]]
    assert growth == sorted(growth), "more risk allowed, no less growth"
    assert all(abs(p["weights"].sum() - 1) < 1e-9 for p in pts if p["weights"] is not None)


def test_the_simulated_mean_excess_return_is_the_assumed_one():
    h = _history(T=360)
    f = al.fit_fill(h, ["a", "b"])
    p = al.bootstrap(h, f, n_paths=2000, n_months=120, block=24, rng=np.random.default_rng(6))
    target = np.array([0.05, 0.01, 0.02, 0.0])
    R = p.funds(target, np.array([-0.012, 0.0, 0.0, 0.0]))
    excess = 12 * (R - p.rf[:, :, None]).mean(axis=(0, 1))
    assert excess == pytest.approx(target + np.array([-0.012, 0, 0, 0]), abs=0.003)


def test_a_piece_that_is_not_allowed_gets_no_weight():
    R = _R(S=60)
    allowed = np.array([True, False, True])
    w, _ = al.solve(al.Yearly(R), 3, sigma=0.05, allowed=allowed)
    assert w[1] == 0 and w.sum() == pytest.approx(1)
    pts = al.frontier(al.Never(R), 3, sigmas=(0.05,), allowed=allowed)
    assert all(p["weights"][1] == 0 for p in pts if p["weights"] is not None)

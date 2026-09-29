"""The factor-mimicking constructions recover known factors from synthetic panels."""
import numpy as np

from sdp import factors


def test_zscore_centers_scales_and_fills_a_gap_with_zero():
    rng = np.random.default_rng(0)
    x = rng.normal(5, 2, 1000)
    x[:10] = np.nan
    w = np.ones(1000)
    z = factors.zscore(x, w)
    assert np.all(z[:10] == 0)
    assert abs(z[10:].mean()) < 0.01
    assert abs(z[10:].std() - 1) < 0.02
    assert np.abs(z).max() <= 3


def test_zscore_limits_an_outlier():
    x = np.append(np.random.default_rng(1).normal(0, 1, 999), 1e6)
    z = factors.zscore(x, np.ones(1000))
    assert z[-1] == 3


def test_zscore_centered_on_the_cap_gives_the_cap_weighted_market_no_exposure():
    rng = np.random.default_rng(2)
    cap = np.exp(rng.normal(22, 1.5, 2000))
    x = np.log(cap)
    z = factors.zscore(x, cap, clip=10.0)
    assert abs(np.average(z, weights=cap)) < 1e-9
    assert abs(z.std() - 1) < 1e-9, "the scale stays the equal-weighted deviation"
    assert factors.zscore(x, cap, clip=1.5).max() == 1.5


def test_style_returns_recover_the_factor_returns():
    rng = np.random.default_rng(2)
    days, names, k = 40, 600, 3
    f_true = rng.normal(0, 0.01, (days, k + 1))
    rows_day, rows_x, rows_r, rows_w = [], [], [], []
    for t in range(days):
        X = rng.normal(0, 1, (names, k))
        w = rng.uniform(1, 4, names)
        Z = np.column_stack([factors.zscore(X[:, j], w) for j in range(k)])
        r = f_true[t, 0] + Z @ f_true[t, 1:] + rng.normal(0, 1e-4, names)
        rows_day.append(np.full(names, t))
        rows_x.append(X)
        rows_r.append(r)
        rows_w.append(w)
    d, F, n, r2 = factors.style_returns(np.concatenate(rows_day), np.vstack(rows_x),
                                        np.concatenate(rows_r), np.concatenate(rows_w))
    assert list(d) == list(range(days))
    assert np.all(n == names)
    assert np.abs(F - f_true).max() < 1e-4
    assert np.nanmin(r2) > 0.99


def test_style_returns_skip_a_thin_session():
    X = np.zeros((10, 2))
    d, F, n, _ = factors.style_returns(np.zeros(10), X, np.zeros(10), np.ones(10))
    assert n[0] == 10 and np.isnan(F[0]).all()


def _one_factor_panel(seed=3, T=400, N=200):
    rng = np.random.default_rng(seed)
    m = rng.normal(0, 0.01, T)
    beta = rng.uniform(0.5, 1.5, N)
    R = m[:, None] * beta[None, :] + rng.normal(0, 0.005, (T, N))
    U = np.ones((T, N), dtype=bool)
    A = np.tile(np.arange(N, 0, -1, dtype=float), (T, 1))
    return m, R, U, A


def test_the_first_eigenportfolio_tracks_the_market():
    m, R, U, A = _one_factor_panel()
    F, share, used = factors.pca_returns(R, U, A, window=100, step=20, k=3, n_names=150)
    assert np.isnan(F[:100]).all()
    live = np.isfinite(F[:, 0])
    assert np.corrcoef(F[live, 0], m[live])[0, 1] > 0.95
    assert np.all(share[live, 0] > share[live, 1])
    assert set(used[live]) == {150}


def test_an_eigenportfolio_is_out_of_sample():
    """A shock on one row cannot change the weights that earn that row."""
    _, R, U, A = _one_factor_panel()
    F1, _, _ = factors.pca_returns(R, U, A, window=100, step=20, k=2, n_names=150)
    R2 = R.copy()
    R2[120] += 0.5
    F2, _, _ = factors.pca_returns(R2, U, A, window=100, step=20, k=2, n_names=150)
    assert np.allclose(F1[:120], F2[:120], equal_nan=True)
    assert np.allclose(F1[121:140], F2[121:140], equal_nan=True)


def test_mimic_returns_track_a_target_out_of_sample():
    rng = np.random.default_rng(4)
    T, m = 600, 4
    B = rng.normal(0, 0.01, (T, m))
    b = np.array([0.5, -0.3, 0.2, 0.1])
    y = B @ b + rng.normal(0, 1e-4, T)
    y[-60:] = np.nan  # The target ends early, as a published factor does.
    out, r2 = factors.mimic_returns(y, B, window=252, step=21)
    live = np.isfinite(out)
    assert np.isnan(out[:252]).all()
    assert np.isfinite(out[-60:]).all()
    both = live & np.isfinite(y)
    assert np.corrcoef(out[both], y[both])[0, 1] > 0.99
    assert np.nanmin(r2) > 0.99


def test_industry_returns_recover_market_industries_and_styles():
    """With one dummy per industry, the market is the cap-weighted mean of the industry
    coefficients and the cap-weighted industry returns sum to 0."""
    rng = np.random.default_rng(5)
    days, names, k, g = 30, 900, 2, 4
    rows_day, rows_z, rows_r, rows_w, rows_g, rows_c, truth = [], [], [], [], [], [], []
    for t in range(days):
        grp = rng.integers(0, g, names)
        cap = rng.uniform(1, 100, names)
        Z = rng.normal(0, 1, (names, k))
        ind = rng.normal(0, 0.01, g)
        f = rng.normal(0, 0.01, k)
        r = ind[grp] + Z @ f + rng.normal(0, 1e-5, names)
        share = np.array([cap[grp == j].sum() for j in range(g)]) / cap.sum()
        truth.append((share @ ind, ind - share @ ind, f))
        rows_day.append(np.full(names, t))
        rows_z.append(Z)
        rows_r.append(r)
        rows_w.append(np.sqrt(cap))
        rows_g.append(grp)
        rows_c.append(cap)
    cap = np.concatenate(rows_c)
    d, market, F, G, n, r2 = factors.cross_section_returns(
        np.concatenate(rows_day), np.vstack(rows_z), np.concatenate(rows_r),
        np.concatenate(rows_w), groups=np.concatenate(rows_g), n_groups=g, cap=cap)
    for t, (m, rel, f) in enumerate(truth):
        assert abs(market[t] - m) < 1e-5
        assert np.abs(G[t] - rel).max() < 1e-5
        assert np.abs(F[t] - f).max() < 1e-5
    assert np.nanmin(r2) > 0.99


def test_clip_returns_limits_a_jump_and_leaves_the_rest():
    rng = np.random.default_rng(6)
    r = rng.normal(0, 0.01, 1000)
    r[0] = 0.8
    w = rng.uniform(1, 4, 1000)
    c = factors.clip_returns(r, w, 5.0)
    med = factors.weighted_median(r, w)
    mad = 1.4826 * factors.weighted_median(np.abs(r - med), w)
    assert c[0] == np.float64(med + 5 * mad)
    assert 0.01 < c[0] < 0.1
    assert np.array_equal(c[1:], r[1:])
    assert factors.clip_returns(r, w, 0.0) is r
    assert factors.clip_returns(np.zeros(5), np.ones(5), 5.0).sum() == 0


def test_weighted_median_follows_the_weights():
    x = np.array([1.0, 2.0, 3.0])
    assert factors.weighted_median(x, np.array([1.0, 1.0, 1.0])) == 2.0
    assert factors.weighted_median(x, np.array([1.0, 1.0, 5.0])) == 3.0


def _panel_with_jumps(jumps: bool, seed=7, days=30, names=600, k=3, n_jumps=4):
    """Return a panel with known factor returns. On each session with jumps, the names with
    the largest exposure to the first style earn a return of +50% on top of the model."""
    rng = np.random.default_rng(seed)
    f_true = rng.normal(0, 0.002, (days, k + 1))
    rows_day, rows_z, rows_r, rows_w = [], [], [], []
    for t in range(days):
        w = rng.uniform(1, 4, names)
        Z = rng.normal(0, 1, (names, k))
        r = f_true[t, 0] + Z @ f_true[t, 1:] + rng.normal(0, 1e-3, names)
        if jumps:
            r[np.argsort(Z[:, 0])[-n_jumps:]] += 0.5
        rows_day.append(np.full(names, t))
        rows_z.append(Z)
        rows_r.append(r)
        rows_w.append(w)
    return (np.concatenate(rows_day), np.vstack(rows_z), np.concatenate(rows_r),
            np.concatenate(rows_w), f_true)


def test_return_clip_recovers_the_factor_returns_through_jumps():
    day, Z, r, w, f_true = _panel_with_jumps(jumps=True)
    _, _, F_raw, *_ = factors.cross_section_returns(day, Z, r, w)
    _, _, F_clip, *_ = factors.cross_section_returns(day, Z, r, w, return_clip=5.0)
    err_raw = np.abs(F_raw - f_true[:, 1:]).max()
    err_clip = np.abs(F_clip - f_true[:, 1:]).max()
    assert err_raw > 5e-3, "a jump moves the raw fit"
    assert err_clip < 1.5e-3
    assert err_clip < err_raw / 10


def test_return_clip_agrees_with_the_raw_fit_without_jumps():
    day, Z, r, w, _ = _panel_with_jumps(jumps=False)
    _, m0, F0, _, _, r2_0 = factors.cross_section_returns(day, Z, r, w)
    _, m5, F5, _, _, r2_5 = factors.cross_section_returns(day, Z, r, w, return_clip=5.0)
    assert np.abs(F0 - F5).max() < 1e-9
    assert np.abs(m0 - m5).max() < 1e-9
    assert np.abs(r2_0 - r2_5).max() < 1e-9


def test_return_clip_keeps_the_r2_on_the_raw_returns():
    day, Z, r, w, _ = _panel_with_jumps(jumps=True, days=1)
    _, market, F, _, _, r2 = factors.cross_section_returns(day, Z, r, w, return_clip=5.0)
    A = np.column_stack([np.ones(r.size), Z])
    coef = np.concatenate([market, F[0]])
    resid = r - A @ coef
    mean = np.average(r, weights=w)
    expected = 1 - np.sum(w * resid ** 2) / np.sum(w * (r - mean) ** 2)
    assert abs(r2[0] - expected) < 1e-12
    _, _, _, _, _, r2_raw = factors.cross_section_returns(day, Z, r, w)
    assert r2[0] < r2_raw[0], "the raw fit has the highest R² on the raw returns"

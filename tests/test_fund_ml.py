"""The machine-learning forecast of index returns: features, ranks, the purge and the fits."""
import numpy as np
import pytest

from sdp.fund import ml

JAN_1990 = (1990 - 1970) * 12


def _panel(signal: float, years: int = 21, names: int = 6, seed: int = 0):
    """Return (raw, universe, month, y) of a panel in which the target is signal * mom + noise."""
    rng = np.random.default_rng(seed)
    month, universe = [], []
    for u in range(len(ml.UNIVERSES)):
        for m in range(JAN_1990, JAN_1990 + 12 * years):
            month += [m] * names
            universe += [u] * names
    month, universe = np.array(month), np.array(universe)
    raw = rng.normal(size=(len(month), len(ml.FEATURES)))
    y = signal * raw[:, ml.FEATURES.index("mom")] + rng.normal(size=len(month))
    group = universe * 1_000_000 + month
    for g in np.unique(group):
        y[group == g] -= y[group == g].mean()
    return raw, universe, month, y


class TestFeatures:
    def test_they_match_a_loop_over_the_windows(self):
        rng = np.random.default_rng(1)
        ret = rng.normal(0.008, 0.04, size=90)
        rf = rng.uniform(0.0, 0.004, size=90)
        f = ml.features(ret, rf)
        t = 80
        wealth = np.cumprod(1 + ret[t - 11:t + 1])
        peak = np.maximum.accumulate(np.r_[1.0, wealth])[1:]
        assert f["r3"][t] == pytest.approx(np.prod(1 + ret[t - 2:t + 1]) - 1)
        assert f["r6"][t] == pytest.approx(np.prod(1 + ret[t - 5:t + 1]) - 1)
        assert f["mom"][t] == pytest.approx(np.prod(1 + ret[t - 11:t]) - 1)
        assert f["val"][t] == pytest.approx(1 - np.prod(1 + ret[t - 59:t - 11]))
        assert f["vol12"][t] == pytest.approx(np.std(ret[t - 11:t + 1], ddof=1))
        assert f["dd12"][t] == pytest.approx(np.min(wealth / peak - 1))
        assert f["trend"][t] == pytest.approx(np.prod(1 + ret[t - 11:t + 1])
                                              - np.prod(1 + rf[t - 11:t + 1]))

    def test_a_window_needs_all_its_months(self):
        ret = np.full(80, 0.01)
        f = ml.features(ret, np.zeros(80))
        assert np.isnan(f["r3"][:2]).all() and np.isfinite(f["r3"][2:]).all()
        assert np.isnan(f["mom"][:11]).all() and np.isfinite(f["mom"][11:]).all()
        assert np.isnan(f["val"][:59]).all() and np.isfinite(f["val"][59:]).all()
        ret[40] = np.nan
        g = ml.features(ret, np.zeros(80))
        assert np.isnan(g["r3"][40:43]).all() and np.isfinite(g["r3"][43]).all()
        assert np.isnan(g["dd12"][40:52]).all() and np.isfinite(g["dd12"][52]).all()

    def test_the_drawdown_starts_at_one_and_is_never_positive(self):
        down = np.full(24, -0.02)
        up = np.full(24, 0.02)
        assert ml.features(down, np.zeros(24))["dd12"][20] == pytest.approx(0.98**12 - 1)
        assert ml.features(up, np.zeros(24))["dd12"][20] == 0.0


class TestRankScale:
    def test_the_ends_are_half_either_way_and_ties_share_a_rank(self):
        x = np.array([3.0, 1.0, 2.0, 2.0, 5.0])
        out = ml.rank_scale(x, np.zeros(5, dtype=int))
        assert out[1] == -0.5 and out[4] == 0.5
        assert out[2] == out[3] == pytest.approx((2.5 - 1) / 4 - 0.5)

    def test_each_group_is_ranked_alone_and_a_missing_value_stays_missing(self):
        x = np.array([10.0, 20.0, np.nan, 1.0, 5.0, 3.0, 99.0])
        g = np.array([1, 1, 1, 2, 2, 2, 3])
        out = ml.rank_scale(x, g)
        assert out[:2].tolist() == [-0.5, 0.5]
        assert np.isnan(out[2])
        assert out[3:6].tolist() == [-0.5, 0.5, 0.0]
        assert out[6] == 0.0

    def test_the_matrix_zeroes_a_missing_rank_and_adds_one_indicator_per_universe(self):
        raw = np.tile(np.arange(7, dtype=float), (4, 1))
        raw[:, 3] = [1.0, np.nan, 3.0, 9.0]
        X = ml.matrix(raw, np.array([0, 0, 0, 2]), np.full(4, 5))
        assert X.shape == (4, 7 + 7 + 3)
        assert np.isnan(X[1, 3])
        assert X[:3, 7 + 3].tolist() == [-0.5, 0.0, 0.5]
        assert X[3, 7 + 3] == 0.0
        assert X[:, 14:].tolist() == [[1, 0, 0]] * 3 + [[0, 0, 1]]


class TestStandardizer:
    def test_a_missing_value_becomes_the_training_mean(self):
        X = np.array([[1.0, 5.0], [3.0, np.nan], [5.0, 7.0]])
        s = ml.Standardizer.fit(X)
        Z = s.apply(X)
        assert Z[1, 1] == 0.0
        assert Z[:, 0].mean() == pytest.approx(0.0) and Z[:, 0].std() == pytest.approx(1.0)

    def test_a_constant_or_empty_column_is_not_divided_by_zero(self):
        X = np.array([[2.0, np.nan], [2.0, np.nan]])
        assert np.isfinite(ml.Standardizer.fit(X).apply(X)).all()


class TestWalkForward:
    def _plan(self):
        month = np.arange(JAN_1990, JAN_1990 + 12 * 12)
        y = np.ones(month.size)
        y[-1] = np.nan
        return month, y, list(ml.refit_plan(month, y, 1995, 2001))

    def test_no_training_target_month_reaches_the_first_predicted_month(self):
        month, y, plan = self._plan()
        for year, train, test in plan:
            first = (year - 1970) * 12
            assert (month[train] + 1).max() < first
            assert month[test].min() == first
            assert not (train & test).any()

    def test_the_month_before_the_first_predicted_month_is_purged(self):
        month, y, plan = self._plan()
        year, train, _ = plan[0]
        first = (year - 1970) * 12
        assert month[train].max() == first - 2
        assert not train[month == first - 1].any()

    def test_a_row_without_a_target_never_trains(self):
        month, y, plan = self._plan()
        assert not any(train[np.isnan(y)].any() for _, train, _ in plan)

    def test_there_is_one_refit_a_year_and_each_trains_on_more(self):
        raw, universe, month, y = _panel(0.5, years=14)
        X = ml.matrix(raw, universe, month)
        calls = []

        def spy(kind, Xt, yt, mt):
            calls.append((kind, len(yt), mt.max()))
            return ml.fit(kind, Xt, yt, mt)

        preds, _ = ml.walk_forward(X, y, month, 1998, 2002, models=("ridge",), fit_fn=spy)
        assert [c[0] for c in calls] == ["ridge"] * 5
        sizes = [c[1] for c in calls]
        assert sizes == sorted(sizes) and len(set(sizes)) == 5
        first_of = [(y_ - 1970) * 12 for y_ in range(1998, 2003)]
        assert all(c[2] + 1 < f for c, f in zip(calls, first_of, strict=True))
        year = 1970 + month // 12
        assert np.isfinite(preds["ridge"][(year >= 1998) & (year <= 2002)]).all()
        assert np.isnan(preds["ridge"][(year < 1998) | (year > 2002)]).all()

    def test_a_planted_signal_is_found_by_both_models(self):
        raw, universe, month, y = _panel(0.6)
        X = ml.matrix(raw, universe, month)
        preds, penalties = ml.walk_forward(X, y, month, 2000, 2009)
        assert set(penalties.values()) <= set(ml.PENALTIES)
        for kind in ml.MODELS:
            for u in range(3):
                rows = (universe == u) & np.isfinite(preds[kind])
                stats = ml.evaluate({kind: preds[kind][rows]}, y[rows], month[rows])[kind]
                assert stats["mean_ic"] > 0.25 and stats["t_nw"] > 5
                assert stats["spread_ann"] > 0

    def test_pure_noise_has_an_ic_near_zero(self):
        raw, universe, month, y = _panel(0.0, seed=7)
        X = ml.matrix(raw, universe, month)
        preds, _ = ml.walk_forward(X, y, month, 2000, 2009)
        for kind in ml.MODELS:
            ics = []
            for u in range(3):
                rows = (universe == u) & np.isfinite(preds[kind])
                _, _, ic, _ = ml.monthly_stats(preds[kind][rows], y[rows], month[rows])
                ics.append(ic)
            ics = np.concatenate(ics)
            assert abs(ics.mean()) < 0.08


class TestFit:
    def test_the_ridge_penalty_comes_from_the_grid_and_the_fit_is_repeatable(self):
        raw, universe, month, y = _panel(0.5, years=8)
        X = ml.matrix(raw, universe, month)
        a, b = ml.fit("ridge", X, y, month), ml.fit("ridge", X, y, month)
        assert a.penalty in ml.PENALTIES
        assert np.array_equal(a.predict(X), b.predict(X))

    def test_the_penalty_is_chosen_on_the_last_fifth_of_the_months(self):
        rng = np.random.default_rng(3)
        month = np.repeat(np.arange(100), 5)
        x = rng.normal(size=(month.size, 2))
        y = x[:, 0] + rng.normal(0, 0.1, size=month.size)
        assert ml.choose_penalty(x, y, month) <= 10.0
        # The last 20 months flip the sign: the fit on the first 80 does worse the less it shrinks.
        y[month >= 80] *= -1
        assert ml.choose_penalty(x, y, month) == max(ml.PENALTIES)

    def test_a_tree_model_fits_a_step(self):
        rng = np.random.default_rng(4)
        month = np.repeat(np.arange(200), 5)
        raw = rng.normal(size=(month.size, len(ml.FEATURES)))
        y = np.where(raw[:, 0] > 0, 1.0, -1.0) + rng.normal(0, 0.1, size=month.size)
        X = ml.matrix(raw, np.zeros(month.size, dtype=int), month)
        model = ml.fit("gbt", X, y, month)
        assert np.corrcoef(model.predict(X), y)[0, 1] > 0.9

    def test_an_unknown_model_raises(self):
        with pytest.raises(ValueError, match="unknown"):
            ml.fit("svm", np.zeros((3, 2)), np.zeros(3), np.arange(3))


class TestScores:
    def test_the_combo_is_the_mean_of_the_two_z_scores_of_the_month(self):
        mom = np.array([1.0, 2.0, 3.0, 10.0, 20.0, 30.0])
        val = np.array([3.0, 2.0, 1.0, 0.0, 5.0, 10.0])
        month = np.array([0, 0, 0, 1, 1, 1])
        out = ml.combo_score(mom, val, month)
        assert out[:3] == pytest.approx([0.0, 0.0, 0.0])
        assert out[3:] == pytest.approx([-1.0, 0.0, 1.0])

    def test_a_row_without_val_has_no_combo(self):
        out = ml.combo_score(np.array([1.0, 2.0, 3.0]), np.array([1.0, np.nan, 3.0]),
                             np.zeros(3, dtype=int))
        assert np.isnan(out[1]) and np.isfinite(out[[0, 2]]).all()

    def test_the_spread_cuts_terciles_like_ntile(self):
        rank = np.arange(1.0, 8.0)
        months, n, ic, spread = ml.monthly_stats(rank, rank, np.zeros(7, dtype=int))
        assert n.tolist() == [7.0] and ic[0] == pytest.approx(1.0)
        # Buckets of 3, 2 and 2 names: the bottom is 1, 2, 3 and the top is 6, 7.
        assert spread[0] == pytest.approx(6.5 - 2.0)

    def test_a_month_with_fewer_than_five_names_is_dropped(self):
        score = np.arange(9.0)
        month = np.array([0] * 4 + [1] * 5)
        months, n, _, _ = ml.monthly_stats(score, score[::-1].copy(), month)
        assert months.tolist() == [1] and n.tolist() == [5.0]

    def test_evaluate_counts_the_months_and_reverses_the_sign_of_a_reversed_score(self):
        month = np.repeat(np.arange(30), 6)
        rng = np.random.default_rng(5)
        target = rng.normal(size=month.size)
        out = ml.evaluate({"up": target, "down": -target}, target, month)
        assert out["up"]["n_months"] == out["down"]["n_months"] == 30
        assert out["up"]["mean_ic"] == pytest.approx(1.0)
        assert out["down"]["mean_ic"] == pytest.approx(-1.0)
        assert out["up"]["spread_ann"] == pytest.approx(-out["down"]["spread_ann"])

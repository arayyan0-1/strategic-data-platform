"""The evaluation protocol: the purge of labels at fold boundaries, the walk-forward
split and the standard errors of overlapping labels."""
import numpy as np
import pytest

from sdp.evaluation import ic_stats, inside, newey_west, nw_lag, summarize, walk_forward


def calendar(sizes: list[int], horizon: int) -> tuple[np.ndarray, np.ndarray]:
    """Folds of the given sizes in date order, and the fold where each label ends."""
    fold = np.repeat(np.arange(len(sizes)), sizes)
    label_fold = np.full(fold.size, -1)
    label_fold[:-horizon] = fold[horizon:]
    return fold, label_fold


def test_the_lag_covers_the_overlap_of_the_labels():
    assert [nw_lag(h) for h in (1, 5, 21)] == [5, 10, 42]


def test_a_label_that_crosses_into_the_next_fold_is_purged():
    fold, label_fold = calendar([3, 10, 10, 4], horizon=5)
    use = inside(fold, label_fold, [1])
    assert use.sum() == 10 - 5
    assert set(np.flatnonzero(use)) == set(range(3, 8))


def test_development_never_reads_a_label_in_the_holdout():
    fold, label_fold = calendar([3, 10, 10, 4], horizon=5)
    use = inside(fold, label_fold, [1, 2])
    assert not np.isin(label_fold[use], [3]).any()
    assert use.sum() == 20 - 5


def test_a_label_after_the_last_session_is_never_used():
    fold, label_fold = calendar([0, 6, 6, 6], horizon=2)
    assert not inside(fold, label_fold, [3])[-2:].any()
    assert not inside(fold, np.where(label_fold < 0, np.nan, label_fold), [3])[-2:].any()


def test_walk_forward_trains_on_the_past_and_purges_the_boundary():
    h = 5
    fold, label_fold = calendar([2, 30, 30, 30, 30, 8], horizon=h)
    splits = list(walk_forward(fold, label_fold, n_folds=4))
    assert [k for k, _, _ in splits] == [2, 3, 4]
    for k, train, test in splits:
        first_test = np.flatnonzero(fold == k)[0]
        assert not (train & test).any()
        assert np.flatnonzero(train).max() == first_test - h - 1
        assert (label_fold[train] < k).all()
        assert (fold[test] == k).all() and (label_fold[test] == k).all()
        assert train.sum() == 30 * (k - 1) - h
        assert test.sum() == 30 - h


def test_newey_west_with_no_lag_is_the_standard_error_of_independent_values():
    x = np.random.default_rng(0).normal(size=500)
    mean, se = newey_west(x, 0)
    assert mean == pytest.approx(x.mean())
    assert se == pytest.approx(x.std() / np.sqrt(x.size))


def test_overlapping_labels_make_the_naive_error_too_small():
    """The mean of h consecutive shocks has a long-run variance of one shock. The naive
    standard error is too small by about the root of h, and Newey-West recovers it."""
    h, n = 5, 40_000
    e = np.random.default_rng(1).normal(size=n + h)
    x = np.convolve(e, np.ones(h) / h, mode="valid")[:n]
    _, se_naive = newey_west(x, 0)
    _, se_nw = newey_west(x, nw_lag(h))
    assert se_naive * np.sqrt(n) == pytest.approx(1 / np.sqrt(h), rel=0.1)
    assert 0.8 < se_nw * np.sqrt(n) < 1.05


def test_ic_stats_agree_on_independent_days_and_skip_missing_values():
    rng = np.random.default_rng(2)
    ic = rng.normal(0.02, 0.1, size=2_000)
    ic[::97] = np.nan
    s = ic_stats(ic, horizon=1)
    assert s["n_days"] == np.isfinite(ic).sum()
    assert s["t_nw"] == pytest.approx(s["t_naive"], rel=0.15)
    assert abs(s["ic_autocorr_lag1"]) < 0.1


def test_ic_stats_of_a_short_series_are_missing():
    s = ic_stats(np.array([0.1, 0.2]), horizon=5)
    assert s["n_days"] == 2 and np.isnan(s["t_nw"]) and np.isnan(s["mean_ic"])


def test_summarize_gives_each_group_and_period_its_own_sessions():
    fold, label_fold = calendar([0, 40, 40, 20], horizon=5)
    n = fold.size
    session = np.arange(1, n + 1)
    ic = np.random.default_rng(3).normal(0.03, 0.1, size=n)
    group = np.r_[np.zeros(n, int), np.ones(n, int)]
    rows = summarize(group, np.full(2 * n, 5), np.r_[ic, -ic], np.r_[fold, fold],
                     np.r_[label_fold, label_fold], np.r_[session, session],
                     {0: [1, 2], 1: [1], 2: [2], 9: [3]})
    assert rows["group"].tolist() == [0, 0, 0, 0, 1, 1, 1, 1]
    assert rows["n_days"].tolist() == [75, 35, 35, 15] * 2
    assert rows["first"][1] == 1 and rows["last"][1] == 35
    assert rows["mean_ic"][4] == pytest.approx(-rows["mean_ic"][0])
    dev = inside(fold, label_fold, [1, 2])
    assert rows["mean_ic"][0] == pytest.approx(ic[dev].mean())

"""The paper fund: the holding rule, the projection, costs, objectives and the backtest loop."""
import datetime as dt

import numpy as np
import pytest

from sdp.fund import backtest, covariance
from sdp.fund.book import Book
from sdp.fund.data import Panel
from sdp.fund.objectives import STRATEGIES, Problem, project

D0 = dt.date(2025, 1, 2)


def day(n: int) -> dt.date:
    return D0 + dt.timedelta(days=n)


class TestBook:
    def test_a_top_up_locks_the_whole_position_under_last_buy(self):
        b = Book(cash=1000.0, rule="last_buy")
        b.buy("A", 10, day(0), 10.0, 0.0)
        b.buy("A", 5, day(20), 10.0, 0.0)
        assert b.sellable("A", day(31)) == 0.0
        assert b.sellable("A", day(50)) == 15

    def test_only_young_lots_are_locked_per_lot(self):
        b = Book(cash=1000.0, rule="per_lot")
        b.buy("A", 10, day(0), 10.0, 0.0)
        b.buy("A", 5, day(20), 10.0, 0.0)
        assert b.sellable("A", day(31)) == 10
        assert b.sellable("A", day(29)) == 0.0

    def test_a_sale_of_locked_units_raises(self):
        b = Book(cash=1000.0)
        b.buy("A", 10, day(0), 10.0, 0.0)
        with pytest.raises(ValueError, match="free"):
            b.sell("A", 1, day(29), 10.0, 0.0)

    def test_a_sale_takes_the_oldest_lot_first(self):
        b = Book(cash=1000.0, rule="per_lot")
        b.buy("A", 10, day(0), 10.0, 0.0)
        b.buy("A", 5, day(40), 10.0, 0.0)
        b.sell("A", 12, day(80), 11.0, 1.0)
        assert [lot.units for lot in b.lots["A"]] == [3]
        assert b.cash == pytest.approx(1000 - 150 + 132 - 1)

    def test_an_unknown_rule_raises(self):
        with pytest.raises(ValueError, match="rule"):
            Book(cash=0.0, rule="fifo")


class TestProject:
    def test_the_result_sums_to_one_within_the_bounds(self):
        rng = np.random.default_rng(1)
        lower = np.array([0.3, 0.0, 0.0, 0.0])
        upper = np.array([1.0, 0.2, 1.0, 1.0])
        w = project(rng.normal(size=4), lower, upper)
        assert w.sum() == pytest.approx(1.0)
        assert np.all(w >= lower - 1e-12) and np.all(w <= upper + 1e-12)

    def test_feasible_weights_do_not_move(self):
        w = np.array([0.5, 0.3, 0.2])
        assert project(w, np.zeros(3), np.ones(3)) == pytest.approx(w)

    def test_upper_bounds_under_one_leave_cash(self):
        upper = np.array([0.2, 0.3])
        assert project(np.array([1.0, 1.0]), np.zeros(2), upper) == pytest.approx(upper)


def test_trade_cost_is_spread_plus_concave_impact():
    assert backtest.trade_cost(0.0, 1e-4, 0.01, 1e6) == 0.0
    small = backtest.trade_cost(1e3, 1e-4, 0.01, 1e6) / 1e3
    big = backtest.trade_cost(1e5, 1e-4, 0.01, 1e6) / 1e5
    assert small > 1e-4 and big > small
    assert big - 1e-4 == pytest.approx((small - 1e-4) * 10)


def test_active_covariance_is_the_covariance_of_the_differences():
    rng = np.random.default_rng(2)
    X = rng.normal(size=(500, 4))
    S = np.cov(X, rowvar=False)
    direct = np.cov(X - X[:, [2]], rowvar=False)
    assert covariance.active(S, 2) == pytest.approx(direct)


def _problem(n: int = 6, lower: np.ndarray | None = None) -> Problem:
    rng = np.random.default_rng(3)
    common = rng.normal(0, 0.01, size=(300, 1))
    X = common + rng.normal(0, 0.0002, size=(300, n))
    y = X[:, 0].copy()
    S = np.cov(X, rowvar=False)
    Sa = covariance.active(S, 0) + 1e-10 * np.eye(n)
    return Problem(
        names=[f"F{i}" for i in range(n)], X=X, y=y, S=S, S_active=Sa,
        fee=np.linspace(0.003, 0.0005, n), alpha=np.zeros(n), cost=np.full(n, 2e-4),
        w0=np.zeros(n), lower=np.zeros(n) if lower is None else lower, upper=np.ones(n),
        benchmark="F0")


@pytest.mark.parametrize("name", sorted(STRATEGIES))
def test_every_objective_is_long_only_fully_invested_and_keeps_locked_weights(name):
    lower = np.array([0.0, 0.0, 0.0, 0.25, 0.0, 0.0])
    p = _problem(lower=lower)
    w = project(np.asarray(STRATEGIES[name][1](p), dtype=float), p.lower, p.upper)
    assert w.sum() == pytest.approx(1.0, abs=1e-6)
    assert np.all(w >= p.lower - 1e-6)


def test_mvo_te_leaves_the_costly_benchmark_for_cheaper_copies():
    p = _problem()
    w = STRATEGIES["mvo_te"][1](p)
    assert w[0] < 0.99
    assert w @ p.fee < p.fee[0]


def _panel(n_sessions: int = 420) -> Panel:
    rng = np.random.default_rng(4)
    dates, d = [], dt.date(2024, 1, 2)
    while len(dates) < n_sessions:
        if d.weekday() < 5:
            dates.append(d)
        d += dt.timedelta(days=1)
    n = 4
    ret = rng.normal(0.0003, 0.01, size=(n_sessions, n))
    ret[0] = np.nan
    close = 100 * np.cumprod(np.nan_to_num(ret) + 1, axis=0)
    full = np.ones((n_sessions, n))
    return Panel(
        dates=dates, funds=["ACWI", "B", "C", "D"], keys=["k0", "k1", "k2", "k3"],
        ret=ret, adj_open=close * (1 + rng.normal(0, 0.001, size=close.shape)),
        adj_close=close, eligible=full.astype(bool), half_spread=full * 1e-4,
        sigma=full * 0.01, adv=full * 1e8, fee=np.array([0.003, 0.0003, 0.0005, 0.0009]),
        rf=np.full(n_sessions, 1e-4), benchmark="ACWI", bench_ret=ret[:, 0].copy(),
        holdout_start=dates[-1])


def test_decision_rows_are_the_last_session_of_each_month():
    p = _panel()
    rows = backtest.decision_rows(p, p.dates[0], p.dates[-1])
    assert all(p.dates[i + 1].month != p.dates[i].month for i in rows)
    assert len(rows) == len({(d.year, d.month) for d in p.dates}) - 1


@pytest.mark.parametrize("name", ["benchmark", "equal_weight", "mvo_te"])
@pytest.mark.parametrize("rule", ["last_buy", "per_lot"])
def test_a_backtest_runs_and_never_breaks_the_holding_rule(name, rule):
    p = _panel()
    cfg = backtest.Config(strategy=name, rule=rule, window=126, covariance="sample")
    r = backtest.run(p, cfg, p.dates[130], p.dates[-1])
    assert len(r.nav) == len(r.dates) and np.all(r.nav > 0)
    assert not any(i["failed"] for i in r.rebalances)
    s = backtest.summary(r)
    assert np.isfinite(s["return"]) and s["turnover"] >= 0


def test_holding_the_benchmark_tracks_it():
    p = _panel()
    cfg = backtest.Config(strategy="benchmark", window=126, covariance="sample")
    s = backtest.summary(backtest.run(p, cfg, p.dates[130], p.dates[-1]))
    assert s["tracking_error"] < 0.002

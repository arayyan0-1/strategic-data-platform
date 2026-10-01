"""The views of one stock and a pair (sdp.stock): the search index, the profile, the pair,
the cache on the build stamp and the routes. The warehouse is a small synthetic file with
two related stocks, A and B, over 320 sessions. No socket, no network."""
import datetime as dt
import json
import math

import duckdb
import numpy as np
import pytest

from sdp import risk, stock, transform
from sdp.config import settings
from tests.test_dashboard import _post
from tests.test_market import _get

N = 320
STYLES = stock.STYLES
INDUSTRIES = stock.INDUSTRIES


def _stamp(when: str) -> None:
    transform.stamp_path().write_text(json.dumps({"published_utc": when, "argv": ["build"]}))


FUND_MACRO = ("d_t_10y", "d_dollar")


@pytest.fixture
def warehouse(tmp_data_root):
    """Write the tables that the views read. B is A plus noise, so the two are related. C is
    an ADR in coverage with no cap and no industry. F and G are funds with the same loadings,
    and P is a preferred with no model. The last two sessions have a pending dollar change."""
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.warehouse_path))
    for schema in ("core", "intermediate", "factors", "research", "reference"):
        con.execute(f"create schema {schema}")
    rng = np.random.default_rng(7)
    days = [dt.date(2025, 1, 1) + dt.timedelta(days=i) for i in range(N)]
    f = rng.normal(0, 0.01, N)
    ra = f + rng.normal(0, 0.01, N)
    rb = 0.8 * ra + rng.normal(0, 0.005, N)
    rc = 0.6 * f + rng.normal(0, 0.01, N)
    rf = 0.9 * f + rng.normal(0, 0.002, N)
    rg = 0.85 * f + rng.normal(0, 0.002, N)
    rp = rng.normal(0, 0.003, N)
    series = (("KA", "AAA", "CS", 100, ra), ("KB", "BBB", "CS", 50, rb),
              ("KC", "CCC", "ADRC", 40, rc), ("KF", "FFF", "ETF", 80, rf),
              ("KG", "GGG", "ETF", 60, rg), ("KP", "PPPpA", "PFD", 25, rp))
    rows = [(k, t, ty, d, p0 * float(np.exp(np.cumsum(rets)[i])), float(rets[i]))
            for k, t, ty, p0, rets in series for i, d in enumerate(days)]
    con.execute("""create table px(
        security_key varchar, ticker varchar, type varchar, date date, px double, ret double)""")
    con.executemany("insert into px values (?, ?, ?, ?, ?, ?)", rows)
    con.execute("""create table core.securities as
        select distinct security_key, ticker, ticker || ' Inc' as name, type,
               'share_class_figi' as key_rule, date '2025-01-01' as first_date, ticker as tickers
        from px""")
    con.execute("""create table intermediate.int_universe as
        select date, ticker, security_key, true as is_primary_line, ticker || ' Inc' as name,
               type as type_filled, px as close, 1e8 as dollar_volume, 1e8 as adv,
               case ticker when 'AAA' then 2e11 when 'BBB' then 1e11 end as market_cap,
               case when type = 'CS' then 'Hlth' else 'Unknown' end as industry,
               case when type = 'CS' then 'Health care' else 'Unknown' end as industry_name,
               2834 as sic_code, 'XNYS' as primary_exchange, type = 'CS' as in_universe
        from px""")
    con.execute("""create table core.security_sessions as
        select security_key, date, ticker, px as adj_close_total, px as adj_close_split,
               1e6 as volume, 1e8 as dollar_volume from px""")
    con.execute("""create table core.rates as
        select date, 0.0001::double as rf from px where security_key = 'KA'""")
    con.execute("""create table intermediate.int_security_details as
        select distinct security_key, date '2025-10-31' as month_end, 1e9 as shares,
               'Pharmaceutical preparations' as sic_description from px""")
    zs = ", ".join(f"0.5 as z_{s}" for s in STYLES)
    con.execute(f"""create table factors.style_exposures as
        select security_key, date, 'Hlth' as industry, {zs} from px where type = 'CS'""")
    parts = ", ".join(f"0.0 as {s}" for s in STYLES)
    inds = ", ".join(f"0.0 as ind_{i}" for i in INDUSTRIES)
    con.execute(f"""create table factors.style_factor_returns as
        select date, ret as market, {parts}, {inds} from px where security_key = 'KA'""")
    con.execute("""create table factors.style_residuals as
        select security_key, date, ret, ret * 0.5 as market_part, 0.0 as industry_part,
               ret * 0.5 as resid, 0.01 as spec_vol, ret * 50 as resid_z,
               market_cap as weight_cap
        from px join intermediate.int_universe using (security_key, date, ticker)
        where in_universe""")
    zc = ", ".join(f"0.3 as z_{s}" for s in STYLES)
    zc = zc.replace("0.3 as z_size", "0.0 as z_size").replace("0.3 as z_liquidity",
                                                              "0.0 as z_liquidity")
    con.execute(f"""create table factors.coverage_exposures as
        select security_key, ticker, date, type as type_filled, 'Unknown' as industry,
               null::double as market_cap, {zc}, null::double as fwd_ret_1
        from px where security_key = 'KC'""")
    con.execute("""create table factors.coverage_residuals as
        select security_key, ticker, date, type as type_filled, 'Unknown' as industry,
               null::double as market_cap, ret, ret * 0.4 as market_part, 0.0 as industry_part,
               0.0 as style_part, ret * 0.6 as resid, 0.01 as own_vol, 1.0 as vol_regime,
               0.01 as spec_vol, ret * 60 as resid_z
        from px where security_key = 'KC'""")
    con.execute(f"""create table factors.macro_factor_returns as
        select date, 0.05 * sin(ret * 300) as d_t_10y,
               case when date >= date '{days[-2]}' then null else 0.003 * cos(ret * 200) end
                   as d_dollar,
               case when date >= date '{days[-2]}' then ['d_dollar'] else []::varchar[] end
                   as pending
        from px where security_key = 'KA'""")
    loads = ", ".join(f"{v}::double as {c}" for c, v in (
        ("market", "{m}"), *((s, "0.1") for s in STYLES),
        *((f"ind_{i}", "0.4" if i == "hlth" else "0.01") for i in INDUSTRIES),
        ("d_t_10y", "{d}"), ("d_dollar", "{u}")))
    fits = []
    for key, tkr, mkt, dur, usd in (("KF", "FFF", 0.9, -0.15, -0.3),
                                    ("KG", "GGG", 0.85, -0.12, -0.25)):
        for fit, last in ((days[20], days[199]), (days[200], days[N - 1])):
            fits.append(f"""select '{key}' as security_key, '{tkr}' as ticker,
                date '{fit}' as fit_date, date '{fit - dt.timedelta(days=1)}' as window_end,
                date '{last}' as last_date, 0.97::double as r2_in, 252 as n_obs,
                5.0::double as ridge, 0.0001::double as intercept,
                {loads.format(m=mkt, d=dur, u=usd)}""")
    con.execute("create table factors.fund_exposures as " + " union all ".join(fits))
    macro_parts = " + ".join(f"coalesce(e.{m} * x.{m}, 0)" for m in FUND_MACRO)
    con.execute(f"""create table factors.fund_residuals as
        with p as (
            select r.security_key, r.ticker, r.date, e.fit_date, r.ret, e.intercept as alpha,
                   e.market * f.market as market_part, 0.0 as industry_part,
                   0.0 as style_part, {macro_parts} as macro_part,
                   list_has_any(x.pending, ['d_dollar', 'd_t_10y']) as macro_pending
            from px r
            join factors.fund_exposures e
                on e.security_key = r.security_key and r.date between e.fit_date and e.last_date
            join factors.style_factor_returns f on f.date = r.date
            join factors.macro_factor_returns x on x.date = r.date)
        select security_key, ticker, date, fit_date, ret, alpha, market_part, industry_part,
               style_part, macro_part,
               ret - alpha - market_part - industry_part - style_part - macro_part as resid,
               0.004 as spec_vol,
               (ret - alpha - market_part - industry_part - style_part - macro_part) / 0.004
                   as resid_z,
               macro_pending
        from p""")
    con.execute("create table research.signals as select security_key, date, ret as ret_1 from px")
    con.execute("""create table research.signal_snapshot as
        select date, 'momentum_12_1' as signal, ticker, security_key, 0.1 as value, 1.0 as z,
               9 as decile, 10 as rank_high, 2 as n_names
        from px where date = (select max(date) from px) and type = 'CS'""")
    con.execute("""create table reference.factor_labels as
        select 'quintile' as family, 'momentum_12_1' as factor, 'Momentum 12-1 months' as label""")
    con.execute("""create table research.signal_ic_summary as
        select 'momentum_12_1' as signal, 'residual' as target, 21 as horizon,
               'development' as period, 0.01 as mean_ic, 2.0 as t_nw""")
    con.execute("""create table intermediate.int_short_interest as
        select 'AAA' as ticker, max(date) as settlement_date, max(date) as effective_date,
               1e7 as short_interest, 2.5 as days_to_cover from px""")
    con.execute("""create table intermediate.int_corporate_actions as
        select 'AAA' as ticker, max(date) - 30 as event_date, 'dividend' as kind,
               null::double as ratio, 0.5 as cash_amount from px""")
    con.execute("""create table research.spreads as
        select security_key, date, 0.0005 as spread, 0.0 as spread_ar, 0.02 as sigma from px""")
    con.execute("drop table px")
    con.close()
    _stamp("2026-09-25T00:00:00+00:00")
    stock._cache.clear()
    yield
    stock._cache.clear()


def test_the_search_index_lists_the_last_session_most_traded_first(warehouse):
    rows = stock.view("search")["rows"]
    assert {r[0] for r in rows} == {"AAA", "BBB", "CCC", "FFF", "GGG", "PPPpA"}
    json.dumps(rows, allow_nan=False)


def test_a_profile_carries_every_part_and_is_json_safe(warehouse):
    p = stock.view("profile", "aaa")
    json.dumps(p, allow_nan=False)
    assert p["head"]["ticker"] == "AAA" and p["head"]["sic_description"]
    assert p["stats"]["ret_1d"] == pytest.approx(p["prices"]["px"][-1] / p["prices"]["px"][-2] - 1)
    assert {r["kind"] for r in p["related"]} == {"specific", "total"}
    assert p["related"][0]["ticker"] == "BBB", "B is A plus noise"
    assert p["signals"][0]["label"] == "Momentum 12-1 months"
    assert p["short_interest"][0]["share_of_shares"] == pytest.approx(0.01)
    assert p["actions"] and p["attribution"][-1]["span"] == "1y"


def test_the_risk_shares_sum_to_one(warehouse):
    risk = stock.view("profile", "AAA")["risk"]
    assert sum(x["var_share"] for x in risk["parts"]) == pytest.approx(1.0)
    assert risk["vol"] == pytest.approx(math.hypot(risk["factor_vol"], risk["spec_vol"]))


def test_the_factor_risk_weights_recent_sessions_more(warehouse):
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    market = np.array([r[0] for r in con.execute(
        "select market from factors.style_factor_returns order by date").fetchall()])
    con.close()
    window = market[-stock.FACTOR_WINDOW:, None]
    expected = math.sqrt(risk.ew_cov(window, stock.FACTOR_HALF_LIFE)[0, 0] * 252)
    got = stock.view("profile", "AAA")["risk"]["factor_vol"]
    assert got == pytest.approx(expected)
    assert got != pytest.approx(math.sqrt(market[-252:].var(ddof=1) * 252), rel=1e-6)


def test_the_attribution_parts_sum_to_the_return(warehouse):
    a = stock.view("profile", "AAA")["attribution"][0]
    parts = ["market", "industry", *STYLES, "specific"]
    assert sum(a[c] for c in parts) == pytest.approx(a["ret"])


def test_a_pair_gives_the_ratio_the_z_score_and_the_hedge_ratio(warehouse):
    q = stock.view("pair", "AAA", "BBB")
    json.dumps(q, allow_nan=False)
    assert len(q["dates"]) == len(q["ratio"]) == len(q["z"]) == len(q["corr_63"])
    assert q["ratio"][0] == pytest.approx(1.0)
    assert q["stats"]["z_now"] is not None, "320 sessions hold a full year for the z-score"
    assert q["stats"]["corr_1y"] > 0.8 and q["stats"]["beta_1y"] == pytest.approx(1.1, abs=0.2)


def test_a_pair_shows_a_half_life_only_when_the_test_rejects_a_random_walk(warehouse):
    s = stock.view("pair", "AAA", "BBB")["stats"]
    assert isinstance(s["adf_t"], float)
    assert (s["half_life"] is None) == (s["adf_t"] >= stock.ADF_CRITICAL)


def _random_walks(n_walks: int, seed: int, n: int = 252) -> np.ndarray:
    return np.cumsum(np.random.default_rng(seed).normal(0, 0.01, (n_walks, n)), axis=1)


def _ar1(phi: float, seed: int, n: int = 252) -> np.ndarray:
    e = np.random.default_rng(seed).normal(0, 0.01, n)
    x = np.empty(n)
    x[0] = e[0] / math.sqrt(1 - phi ** 2)
    for i in range(1, n):
        x[i] = phi * x[i - 1] + e[i]
    return x


def test_the_dickey_fuller_t_is_the_t_of_the_slope_of_the_change_on_the_level():
    x = _random_walks(1, 5)[0]
    dx, lag = np.diff(x), x[:-1]
    design = np.column_stack([np.ones(len(lag)), lag])
    coef = np.linalg.lstsq(design, dx, rcond=None)[0]
    e = dx - design @ coef
    cov = (e @ e / (len(dx) - 2)) * np.linalg.inv(design.T @ design)
    assert stock.reversion(x)[0] == pytest.approx(coef[1] / math.sqrt(cov[1, 1]))


def test_a_random_walk_has_no_half_life():
    t, half_life = stock.reversion(_random_walks(1, 0)[0])
    assert t > stock.ADF_CRITICAL and half_life is None


def test_an_ar1_with_a_coefficient_of_0_9_has_the_half_life_of_the_coefficient():
    t, half_life = stock.reversion(_ar1(0.9, 0))
    assert t < stock.ADF_CRITICAL
    assert half_life == pytest.approx(math.log(0.5) / math.log(0.9), rel=0.3)


def test_the_test_rejects_a_random_walk_in_about_5_percent_of_samples():
    shown = [stock.reversion(x)[1] is not None for x in _random_walks(1000, 1)]
    assert 0.02 < np.mean(shown) < 0.09, "The slope alone is negative in about 95% of them."


def test_white_noise_has_a_half_life_under_one_session_and_a_constant_has_none():
    t, half_life = stock.reversion(np.random.default_rng(0).normal(0, 0.01, 252))
    assert t < stock.ADF_CRITICAL and 0 <= half_life < 1
    assert stock.reversion(np.ones(100)) == (None, None)


def test_an_unknown_or_malformed_ticker_is_refused(warehouse):
    with pytest.raises(stock.UnknownTicker, match="has held"):
        stock.view("profile", "ZZZ")
    with pytest.raises(stock.UnknownTicker, match="is not a ticker"):
        stock.view("profile", "A'; drop table x")


def test_a_view_is_built_again_only_for_a_new_build(warehouse, monkeypatch):
    calls = []
    real = stock.VIEWS["search"]
    monkeypatch.setitem(stock.VIEWS, "search", lambda wh: calls.append(1) or real(wh))
    stock.view("search")
    stock.view("search")
    assert len(calls) == 1
    _stamp("2026-09-26T00:00:00+00:00")
    stock.view("search")
    assert len(calls) == 2


def test_the_routes_serve_the_views_and_refuse_an_unknown_ticker(warehouse):
    code, body = _get("/market/stock.json?t=AAA")
    assert code == 200 and json.loads(body)["head"]["ticker"] == "AAA"
    code, body = _get("/market/pair.json?a=AAA&b=BBB")
    assert code == 200
    code, body = _get("/market/stock.json?t=ZZZ")
    assert code == 404 and "ZZZ" in json.loads(body)["error"]
    code, _ = _get("/market/nothing.json")
    assert code == 404


# ---------- the routes of a security

def test_each_security_takes_the_route_of_its_last_session(warehouse):
    routes = {t: stock.view("profile", t)["route"] for t in ("AAA", "CCC", "FFF", "PPPpA")}
    assert routes == {"AAA": "universe", "CCC": "coverage", "FFF": "fund", "PPPpA": "none"}


def test_a_universe_profile_keeps_its_parts_and_adds_the_route(warehouse):
    p = stock.view("profile", "AAA")
    assert p["route"] == "universe" and p["fund"] is None and p["coverage"] is None
    assert p["exposures"]["industry"] == "Hlth" and p["exposures"]["z_size"] == 0.5
    assert p["similar_funds"] == [] and "universe" in p["route_note"]
    assert p["cumulative"].keys() == {"dates", "total", "specific"}


def test_the_risk_of_a_universe_name_comes_from_the_style_factors_alone(warehouse):
    """The covariance of a stock has no macro factor, so a pending macro change cannot move it."""
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    names = ["market", *STYLES, *(f"ind_{i}" for i in INDUSTRIES)]
    F = np.array(con.execute(f"select {', '.join(names)} from factors.style_factor_returns "
                             "order by date").fetchall(), dtype=float)
    con.close()
    omega = risk.ew_cov(F[-stock.FACTOR_WINDOW:], stock.FACTOR_HALF_LIFE)
    x = np.zeros(len(names))
    x[0] = 1.0
    x[1:1 + len(STYLES)] = 0.5
    x[names.index("ind_hlth")] = 1.0
    expected = (x * (omega @ x) * 252).sum()
    got = stock.view("profile", "AAA")["risk"]["factor_vol"]
    assert got == pytest.approx(math.sqrt(expected))


def test_a_coverage_name_states_what_the_vendor_lacks(warehouse):
    p = stock.view("profile", "ccc")
    json.dumps(p, allow_nan=False)
    assert p["coverage"] == {"type_filled": "ADRC", "missing_cap": True, "missing_industry": True}
    assert "no market cap" in p["route_note"] and "no industry code" in p["route_note"]
    assert p["exposures"]["z_size"] == 0.0 and p["exposures"]["industry"] == "Unknown"
    assert p["head"]["in_universe"] is False
    assert p["industry_median"] is None, "the names with no industry are no peer group"


def test_a_coverage_name_has_a_risk_split_and_an_attribution_that_add_up(warehouse):
    p = stock.view("profile", "CCC")
    assert sum(x["var_share"] for x in p["risk"]["parts"]) == pytest.approx(1.0, abs=1e-9)
    a = p["attribution"][0]
    assert sum(a[c] for c in ["market", "industry", *STYLES, "specific"]) == pytest.approx(a["ret"])


def test_a_stock_has_related_names_from_coverage_too(warehouse):
    tickers = {r["ticker"] for r in stock.view("profile", "AAA")["related"]}
    assert "CCC" in tickers, "the residual correlation reads the universe and coverage together"


def test_a_fund_shows_its_fit_and_its_loadings(warehouse):
    p = stock.view("profile", "FFF")
    json.dumps(p, allow_nan=False)
    f = p["fund"]
    assert f["r2_in"] == pytest.approx(0.97) and f["market"]["loading"] == pytest.approx(0.9)
    assert f["fit_date"] == "2025-07-20" and f["earlier_fit_date"] is None
    macro = {m["factor"]: m for m in f["macro"]}
    assert macro["d_t_10y"]["loading"] == pytest.approx(-0.15)
    assert macro["d_t_10y"]["kind"] == "yield" and macro["d_dollar"]["kind"] == "price"
    assert set(macro) == set(FUND_MACRO)
    noted = {i["factor"] for i in f["industries"] if i["note"]}
    assert "hlth" in noted and len(noted) == 3, "the three largest are always of note"
    assert p["exposures"] is None and "R2 is 0.97" in p["route_note"]


def test_the_risk_of_a_fund_uses_the_style_and_macro_factors_on_complete_rows(warehouse):
    con = duckdb.connect(str(settings.warehouse_path), read_only=True)
    names = ["market", *STYLES, *(f"ind_{i}" for i in INDUSTRIES)]
    F = np.array(con.execute(f"""
        select {", ".join("s." + c for c in names)}, x.d_t_10y, x.d_dollar
        from factors.style_factor_returns s join factors.macro_factor_returns x using (date)
        where x.d_t_10y is not null and x.d_dollar is not null order by date""").fetchall(),
        dtype=float)
    fit = con.execute(f"""select {", ".join(names)}, d_t_10y, d_dollar from factors.fund_exposures
        where ticker = 'FFF' order by fit_date desc limit 1""").fetchone()
    con.close()
    assert len(F) == N - 2, "the two pending sessions are out of the panel"
    omega = risk.ew_cov(F[-stock.FACTOR_WINDOW:], stock.FACTOR_HALF_LIFE)
    x = np.array(fit, dtype=float)
    got = stock.view("profile", "FFF")["risk"]
    assert got["factor_vol"] == pytest.approx(math.sqrt((x * (omega @ x) * 252).sum()))
    assert sum(q["var_share"] for q in got["parts"]) == pytest.approx(1.0, abs=1e-9)
    assert {"d_t_10y", "d_dollar", "market", "industry", "specific"} <= {
        q["component"] for q in got["parts"]}


def test_the_attribution_of_a_fund_adds_alpha_the_macro_parts_and_the_pending_days(warehouse):
    p = stock.view("profile", "FFF")
    a = p["attribution"][0]
    parts = ["alpha", "market", "industry", *STYLES, *FUND_MACRO, "specific"]
    assert sum(a[c] for c in parts) == pytest.approx(a["ret"], abs=1e-12)
    assert a["pending"] == {"d_dollar": 2}
    assert a["alpha"] == pytest.approx(0.0001 * 21)
    assert len(p["cumulative"]["alpha"]) == len(p["cumulative"]["total"])


def test_a_fund_has_the_funds_and_stocks_that_move_with_it(warehouse):
    p = stock.view("profile", "FFF")
    funds = [r for r in p["related"] if r["kind"] == "fund"]
    assert [r["ticker"] for r in funds] == ["GGG"] and funds[0]["ret_corr"] > 0.9
    assert {r["kind"] for r in p["related"]} == {"fund", "stock"}
    assert p["similar_funds"][0]["ticker"] == "GGG" and p["similar_funds"][0]["cosine"] > 0.99


def test_a_security_with_no_model_has_price_statistics_and_no_error(warehouse):
    p = stock.view("profile", "pppPA")
    json.dumps(p, allow_nan=False)
    assert p["route"] == "none" and p["head"]["ticker"] == "PPPpA"
    assert p["risk"] is None and p["exposures"] is None and p["attribution"] == []
    assert p["related"] == [] and p["stats"]["ret_1d"] is not None
    assert "price statistics only" in p["route_note"]


# ---------- the portfolio

def test_a_line_is_a_percent_a_fraction_or_a_money_value():
    for amounts in ([60, 30, 10], [0.6, 0.3, 0.1], [6000, 3000, 1000]):
        lines = stock.parse_holdings([{"ticker": t, "amount": a}
                                      for t, a in zip(("VT", "QQQ", "cash"), amounts, strict=True)])
        assert [round(ln["weight"], 12) for ln in lines] == [0.6, 0.3, 0.1]
        assert [ln["cash"] for ln in lines] == [False, False, True]
        assert lines[2]["ticker"] == "CASH"


def test_a_repeated_ticker_adds_its_amounts():
    lines = stock.parse_holdings([{"ticker": "VT", "amount": 1}, {"ticker": "vt", "amount": 3}])
    assert len(lines) == 1 and lines[0]["weight"] == 1.0 and lines[0]["amount"] == 4.0


@pytest.mark.parametrize("raw", [
    None, [], "VT 60", [{"ticker": "VT"}], [{"amount": 1}], [{"ticker": "VT", "amount": "60"}],
    [{"ticker": "VT", "amount": True}], [{"ticker": "VT", "amount": float("nan")}],
    [{"ticker": "VT", "amount": 0}], [["VT", 1]], [{"ticker": "VT", "amount": 1}] * 61])
def test_a_malformed_request_is_refused(raw):
    with pytest.raises(stock.BadHoldings):
        stock.parse_holdings(raw)


def test_the_exposures_of_a_portfolio_are_the_weighted_sum_of_its_holdings():
    rng = np.random.default_rng(3)
    w, X = rng.dirichlet(np.ones(5)), rng.normal(0, 1, (5, 9))
    assert stock.combine(w, X) == pytest.approx(sum(w[i] * X[i] for i in range(5)))


def test_the_variance_split_sums_to_the_total_by_factor_and_by_holding():
    rng = np.random.default_rng(4)
    w, X = rng.dirichlet(np.ones(6)), rng.normal(0, 1, (6, 9))
    A = rng.normal(0, 0.01, (9, 9))
    omega, sv = A @ A.T, rng.uniform(1e-5, 1e-4, 6)
    v = stock.variance_split(w, X, omega, sv)
    x = w @ X
    direct = (x @ omega @ x + (w ** 2 * sv).sum()) * 252
    assert v["total"] == pytest.approx(direct, rel=1e-12)
    assert v["by_factor"].sum() + v["specific"].sum() == pytest.approx(v["total"], rel=1e-12)
    assert v["by_holding"].sum() == pytest.approx(v["total"], rel=1e-12)


def _book(*pairs):
    return [{"ticker": t, "amount": a} for t, a in pairs]


def test_a_portfolio_sums_its_risk_two_ways_and_its_moves_to_the_return(warehouse):
    p = stock.portfolio_view(_book(("AAA", 40), ("FFF", 30), ("CCC", 10), ("CASH", 20)))
    json.dumps(p, allow_nan=False)
    r = p["risk"]
    assert sum(x["var_share"] for x in r["parts"]) == pytest.approx(1.0, abs=1e-9)
    assert sum(x["share"] for x in r["holdings"]) == pytest.approx(1.0, abs=1e-9)
    assert sum(x["variance"] for x in r["holdings"]) == pytest.approx(r["vol"] ** 2, abs=1e-9)
    assert {h["route"] for h in p["holdings"]} == {"universe", "fund", "coverage", "cash"}
    for m in p["moves"]:
        assert sum(v for k, v in m["parts"].items() if k != "ret") == pytest.approx(
            m["parts"]["ret"], abs=1e-12)
    assert [m["span"] for m in p["moves"]][:2] == ["1d", "1w"]
    one_day = p["moves"][0]
    assert one_day["pending"] == ["d_dollar"]
    assert one_day["parts"]["cash"] == pytest.approx(0.2 * 0.0001)
    assert {c["ticker"] for c in one_day["contributors"]} == {"AAA", "FFF", "CCC", "CASH"}


def test_the_exposures_of_a_portfolio_equal_the_weighted_loadings(warehouse):
    p = stock.portfolio_view(_book(("FFF", 50), ("GGG", 30), ("CASH", 20)))
    e = p["exposures"]
    assert e["market"] == pytest.approx(0.5 * 0.9 + 0.3 * 0.85)
    macro = {m["factor"]: m["x"] for m in e["macro"]}
    assert macro["d_t_10y"] == pytest.approx(0.5 * -0.15 + 0.3 * -0.12)
    assert {s["factor"]: s["x"] for s in e["styles"]}["size"] == pytest.approx(0.8 * 0.1)


def test_a_stock_has_no_macro_exposure_and_a_market_exposure_of_its_weight(warehouse):
    p = stock.portfolio_view(_book(("AAA", 1), ("BBB", 1)))
    assert p["exposures"]["market"] == pytest.approx(1.0)
    assert all(m["x"] == 0 for m in p["exposures"]["macro"])


def test_an_unknown_ticker_is_reported_and_its_weight_is_left_out(warehouse):
    p = stock.portfolio_view(_book(("VWRL", 50), ("AAA", 50), ("nope!", 10), ("PPPpA", 10)))
    lost = {h["input"]: h for h in p["holdings"] if h["status"] == "unresolved"}
    assert set(lost) == {"VWRL", "nope!"}
    assert "UK-listed" in lost["VWRL"]["reason"] and "for example VT" in lost["VWRL"]["reason"]
    assert "not a ticker" in lost["nope!"]["reason"]
    assert p["covered_weight"] == pytest.approx(60 / 120)
    assert any("did not resolve" in w for w in p["warnings"])
    pref = next(h for h in p["holdings"] if h["input"] == "PPPpA")
    assert pref["status"] == "no_model" and pref["vol"] > 0


def test_a_portfolio_with_no_resolved_holding_has_no_risk(warehouse):
    p = stock.portfolio_view(_book(("VWRL", 1)))
    assert p["risk"] is None and p["exposures"] is None and p["moves"][0]["parts"]["ret"] == 0


def test_a_portfolio_request_is_never_cached(warehouse):
    before = len(stock._cache)
    stock.portfolio_view(_book(("AAA", 1)))
    stock.portfolio_view(_book(("AAA", 1)))
    assert len(stock._cache) == before


def test_the_route_serves_a_portfolio_and_reports_an_unknown_ticker(warehouse):
    code, body = _post("/market/portfolio.json",
                       json.dumps({"holdings": _book(("FFF", 60), ("ZZZ", 40))}))
    assert code == 200
    out = json.loads(body)
    assert out["holdings"][1]["status"] == "unresolved" and out["risk"]["vol"] > 0
    code, body = _post("/market/portfolio.json", json.dumps({"holdings": []}))
    assert code == 400 and "list of lines" in json.loads(body)["error"]

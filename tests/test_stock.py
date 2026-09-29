"""The views of one stock and a pair (sdp.stock): the search index, the profile, the pair,
the cache on the build stamp and the routes. The warehouse is a small synthetic file with
two related stocks, A and B, over 320 sessions. No socket, no network."""
import datetime as dt
import json
import math

import duckdb
import numpy as np
import pytest

from sdp import stock, transform
from sdp.config import settings
from tests.test_market import _get

N = 320
STYLES = stock.STYLES
INDUSTRIES = stock.INDUSTRIES


def _stamp(when: str) -> None:
    transform.stamp_path().write_text(json.dumps({"published_utc": when, "argv": ["build"]}))


@pytest.fixture
def warehouse(tmp_data_root):
    """Write the tables that the views read. B is A plus noise, so the two are related."""
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.warehouse_path))
    for schema in ("core", "intermediate", "factors", "research", "reference"):
        con.execute(f"create schema {schema}")
    rng = np.random.default_rng(7)
    days = [dt.date(2025, 1, 1) + dt.timedelta(days=i) for i in range(N)]
    f = rng.normal(0, 0.01, N)
    ra = f + rng.normal(0, 0.01, N)
    rb = 0.8 * ra + rng.normal(0, 0.005, N)
    pa, pb = 100 * np.exp(np.cumsum(ra)), 50 * np.exp(np.cumsum(rb))
    rows = [(k, t, d, p, r) for k, t, series, rets in (("KA", "AAA", pa, ra), ("KB", "BBB", pb, rb))
            for d, p, r in zip(days, series, rets, strict=True)]
    con.execute("""create table px(
        security_key varchar, ticker varchar, date date, px double, ret double)""")
    con.executemany("insert into px values (?, ?, ?, ?, ?)", rows)
    con.execute("""create table core.securities as
        select distinct security_key, ticker, ticker || ' Inc' as name, 'CS' as type,
               'share_class_figi' as key_rule, date '2025-01-01' as first_date, ticker as tickers
        from px""")
    con.execute("""create table intermediate.int_universe as
        select date, ticker, security_key, true as is_primary_line, ticker || ' Inc' as name,
               'CS' as type_filled, px as close, 1e8 as dollar_volume, 1e8 as adv,
               case when ticker = 'AAA' then 2e11 else 1e11 end as market_cap,
               'Hlth' as industry, 'Health care' as industry_name, 2834 as sic_code,
               'XNYS' as primary_exchange, true as in_universe
        from px""")
    con.execute("""create table core.security_sessions as
        select security_key, date, ticker, px as adj_close_total, px as adj_close_split,
               1e6 as volume, 1e8 as dollar_volume from px""")
    con.execute("""create table intermediate.int_security_details as
        select distinct security_key, date '2025-10-31' as month_end, 1e9 as shares,
               'Pharmaceutical preparations' as sic_description from px""")
    zs = ", ".join(f"0.5 as z_{s}" for s in STYLES)
    con.execute(f"""create table factors.style_exposures as
        select security_key, date, 'Hlth' as industry, {zs} from px""")
    parts = ", ".join(f"0.0 as {s}" for s in STYLES)
    inds = ", ".join(f"0.0 as ind_{i}" for i in INDUSTRIES)
    con.execute(f"""create table factors.style_factor_returns as
        select date, ret as market, {parts}, {inds} from px where security_key = 'KA'""")
    con.execute("""create table factors.style_residuals as
        select security_key, date, ret, ret * 0.5 as market_part, 0.0 as industry_part,
               ret * 0.5 as resid, 0.01 as spec_vol, ret * 50 as resid_z,
               market_cap as weight_cap
        from px join intermediate.int_universe using (security_key, date, ticker)""")
    con.execute("create table research.signals as select security_key, date, ret as ret_1 from px")
    con.execute("""create table research.signal_snapshot as
        select date, 'momentum_12_1' as signal, ticker, security_key, 0.1 as value, 1.0 as z,
               9 as decile, 10 as rank_high, 2 as n_names
        from px where date = (select max(date) from px)""")
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
    assert {r[0] for r in rows} == {"AAA", "BBB"}
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

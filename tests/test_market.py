"""The market monitor: the payload from the marts, the cache on the build stamp, and the
two routes. The warehouse is a small synthetic file. No socket, no network."""
import datetime as dt
import json
from types import SimpleNamespace

import duckdb
import pytest

from sdp import dashboard, market, transform
from sdp.config import settings

D = dt.date


def _stamp(when: str) -> None:
    transform.stamp_path().write_text(json.dumps({"published_utc": when, "argv": ["build"]}))


@pytest.fixture
def warehouse(tmp_data_root):
    """Write a warehouse with a few rows in each mart that the monitor reads."""
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.warehouse_path))
    con.execute("create schema monitor")
    con.execute("""create table monitor.market_board as
        select 'SPY' as ticker, 'US equity' as asset_group, 'S&P 500' as label, 1 as position,
               'SPDR' as name, date '2026-09-24' as last_date, 700.0 as close,
               0.01 as ret_1d, 0.02 as ret_1w, 'nan'::double as ret_1m, 0.05 as ret_3m,
               0.06 as ret_6m, 0.1 as ret_ytd, 0.2 as ret_1y, -0.01 as off_52w_high,
               0.3 as over_52w_low, 0.12 as vol_20, 1.1 as rel_volume""")
    con.execute("""create table monitor.market_history as
        select 'SPY' as ticker, date '2026-09-23' + cast(i as integer) as date,
               100.0 + i as adj_close
        from range(2) t(i)""")
    con.execute("""create table monitor.market_breadth as
        select date '2026-09-24' as date, 3000 as n_names, 1600 as advancers, 1300 as decliners,
               0.004 as ew_ret, 0.003 as cap_ret, 0.001 as median_ret, 0.02 as dispersion,
               0.6 as pct_above_ma50, 0.55 as pct_above_ma200, 50 as new_highs, 20 as new_lows,
               0.6 as up_volume_share, 300 as ad_line, 1.2 as ew_index, 1.1 as cap_index,
               0.14 as ew_vol_21, 0.2 as avg_corr_21""")
    con.execute("""create table monitor.market_exceptions as
        select date '2026-09-24' as date, * from (values
            ('factor', 'style: momentum', 'momentum factor -3.1 sigma', 3.1, -1.0),
            ('cross-asset', 'SPY', 'SPY -2.2 sigma', 2.2, -1.0),
            ('regime', 'avg_corr_21', 'correlation at the 97th percentile', 2.4, -1.0)
        ) t(area, subject, message, score, direction)""")
    con.execute("""create table monitor.market_rates as
        select * from (values
            (2, 't_10y', 'Treasury 10 years', 'Nominal curve', 'pct', 120, 'high', 4.1, 0.03),
            (1, 't_2y', 'Treasury 2 years', 'Nominal curve', 'pct', 24, 'high', 3.6, 'nan'::double)
        ) t(position, measure, label, grp, unit, maturity, stress, value, chg_1d)""")
    con.execute("""create table monitor.market_rates_history as
        select 't_10y' as measure, date '2026-09-23' + cast(i as integer) as date, 4.0 + i as value
        from range(2) t(i)""")
    con.close()
    _stamp("2026-09-25T00:00:00+00:00")
    market._cache.update(stamp=None, payload=None)
    yield
    market._cache.update(stamp=None, payload=None)


def test_the_payload_carries_json_safe_values(warehouse):
    p = market.snapshot()
    row = p["board"]["rows"][0]
    assert row["ticker"] == "SPY"
    assert row["ret_1m"] is None, "a NaN must become null, or JSON.parse fails"
    assert row["last_date"] == "2026-09-24"
    assert row["series"] == [100.0, 101.0]
    assert p["as_of"] == "2026-09-24"
    json.dumps(p, allow_nan=False)


def test_the_rates_come_in_order_with_their_history_on_one_date_axis(warehouse):
    rates = market.snapshot()["rates"]
    rows = rates["rows"]
    assert [r["measure"] for r in rows] == ["t_2y", "t_10y"]
    assert rows[0]["chg_1d"] is None and rows[0]["series"] == []
    assert rows[1]["series"] == [4.0, 5.0] and rates["dates"] == ["2026-09-23", "2026-09-24"]


def test_a_missing_mart_empties_its_section_and_names_the_fix(warehouse):
    p = market.snapshot()
    assert p["factors"] is None and p["movers"] is None
    assert any("sdp.transform build" in e for e in p["errors"])


def test_the_payload_is_built_again_only_for_a_new_build(warehouse, monkeypatch):
    calls = []
    real = market.build
    monkeypatch.setattr(market, "build", lambda wh: calls.append(1) or real(wh))
    market.snapshot()
    market.snapshot()
    assert len(calls) == 1
    _stamp("2026-09-26T00:00:00+00:00")
    market.snapshot()
    assert len(calls) == 2


def test_no_warehouse_gives_an_error_and_no_crash(tmp_data_root):
    market._cache.update(stamp=None, payload=None)
    p = market.snapshot()
    assert p["as_of"] is None and "warehouse is absent" in p["errors"][0]


def _get(path: str, host: str = "localhost:8787"):
    sent = []
    fake = SimpleNamespace(
        path=path, headers={"Host": host},
        server=SimpleNamespace(server_address=("127.0.0.1", 8787)),
        _send=lambda code, body, *a: sent.append((code, body)),
    )
    fake._refuse = lambda: dashboard.Handler._refuse(fake)
    fake._view = lambda: dashboard.Handler._view(fake)
    dashboard.Handler.do_GET(fake)
    return sent[0]


def test_the_page_and_the_payload_are_served(warehouse):
    code, body = _get("/market")
    assert code == 200 and "market monitor" in body
    code, body = _get("/market.json")
    assert code == 200 and json.loads(body)["as_of"] == "2026-09-24"


def test_a_foreign_host_cannot_read_the_payload(warehouse):
    code, _ = _get("/market.json", host="evil.example:8787")
    assert code == 403


def test_the_exceptions_come_most_unusual_first(warehouse):
    e = market.snapshot()["exceptions"]
    assert [x["score"] for x in e] == [3.1, 2.4, 2.2]


def _short_interest_warehouse() -> None:
    """Add the short interest mart and the universe: twelve small caps with the most days to
    cover, then twelve mid caps, then twelve large caps, and one name with no cap."""
    names = ([(f"S{i:02d}", 1e8, 50.0 - i, 2.0 - i / 100) for i in range(12)]
             + [(f"M{i:02d}", 5e9, 20.0 - i, 1.0 - i / 100) for i in range(12)]
             + [(f"L{i:02d}", 2e10, 5.0 - i / 10, 0.5 - i / 100) for i in range(12)]
             + [("NOCAP", None, 90.0, 3.0)])
    con = duckdb.connect(str(settings.warehouse_path))
    con.execute("create schema intermediate")
    con.execute("create table intermediate.int_universe (date date, ticker varchar, "
                "market_cap double, in_universe boolean)")
    con.execute("""create table monitor.market_short_interest (
        settlement_date date, effective_date date, ticker varchar, name varchar, close double,
        short_interest double, prev_short_interest double, change double, days_to_cover double,
        days_to_cover_computed double, short_value double)""")
    for t, cap, dtc, change in names:
        con.execute("insert into intermediate.int_universe values (date '2026-09-24', ?, ?, true)",
                    [t, cap])
        con.execute("insert into monitor.market_short_interest values "
                    "(date '2026-09-15', date '2026-09-25', ?, ?, 10.0, 2e6, 1e6, ?, ?, ?, 2e7)",
                    [t, t + " Inc", change, dtc, dtc])
    con.close()


def test_a_cap_floor_keeps_ten_names_when_the_top_rows_are_small_caps(warehouse):
    _short_interest_warehouse()
    s = market.snapshot()["short_interest"]
    for key in ("days_to_cover", "increase"):
        rows = s[key]
        assert all("cap" in r for r in rows)
        for floor in (0, 2e9, 1e10):
            kept = [r for r in rows if floor <= 0 or (r["cap"] or 0) >= floor]
            assert len(kept) >= 10, (key, floor)
    top = [r["ticker"] for r in s["days_to_cover"]]
    assert top[:2] == ["NOCAP", "S00"]
    large = [r["ticker"] for r in s["days_to_cover"] if (r["cap"] or 0) >= 1e10]
    assert large[:2] == ["L00", "L01"]
    json.dumps(s, allow_nan=False)


def test_top_by_cap_keeps_the_order_and_drops_a_missing_cap_from_a_floor():
    rows = [{"ticker": "A", "cap": None}, {"ticker": "B", "cap": 3e9}, {"ticker": "C", "cap": 1e8}]
    assert [r["ticker"] for r in market._top_by_cap(rows, floors=(0, 2e9), n=1)] == ["A", "B"]
    assert [r["ticker"] for r in market._top_by_cap(rows, floors=(0, 2e9), n=3)] == ["A", "B", "C"]

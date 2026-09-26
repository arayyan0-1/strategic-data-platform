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
    con.execute("create schema main_marts")
    con.execute("""create table main_marts.mart_market_board as
        select 'SPY' as ticker, 'US equity' as asset_group, 'S&P 500' as label, 1 as position,
               'SPDR' as name, date '2026-09-24' as last_date, 700.0 as close,
               0.01 as ret_1d, 0.02 as ret_1w, 'nan'::double as ret_1m, 0.05 as ret_3m,
               0.06 as ret_6m, 0.1 as ret_ytd, 0.2 as ret_1y, -0.01 as off_52w_high,
               0.3 as over_52w_low, 0.12 as vol_20d, 1.1 as rel_volume""")
    con.execute("""create table main_marts.mart_market_history as
        select 'SPY' as ticker, date '2026-09-23' + cast(i as integer) as date, 100.0 + i as px
        from range(2) t(i)""")
    con.execute("""create table main_marts.mart_market_breadth as
        select date '2026-09-24' as date, 3000 as n_names, 1600 as advancers, 1300 as decliners,
               0.004 as ew_ret, 0.003 as dv_ret, 0.001 as median_ret, 0.02 as dispersion,
               0.6 as pct_above_ma50, 0.55 as pct_above_ma200, 50 as new_highs, 20 as new_lows,
               0.6 as up_volume_share, 300 as ad_line, 1.2 as ew_index, 1.1 as dv_index,
               0.14 as ew_vol_21, 0.2 as avg_corr_21""")
    con.execute("""create table main_marts.mart_market_exceptions as
        select date '2026-09-24' as date, * from (values
            ('factor', 'style: momentum', 'momentum factor -3.1 sigma', 3.1, -1.0),
            ('cross-asset', 'SPY', 'SPY -2.2 sigma', 2.2, -1.0),
            ('regime', 'avg_corr_21', 'correlation at the 97th percentile', 2.4, -1.0)
        ) t(area, subject, message, score, direction)""")
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

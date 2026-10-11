"""Tests for the IBKR borrow, ETF screener, NAV history and holdings ingests. No network."""
import datetime as dt
import gzip
import io
import json

import duckdb
import openpyxl
import pytest

from sdp import dal
from sdp.ingest import common, etf_funds, etf_holdings, etf_nav_history, ibkr_borrow
from sdp.ingest.common import EASTERN, AuditFailure


def _ibkr_file(rows: list[str], count: int | None = None, day: str = "2026.10.12") -> bytes:
    head = [f"#BOF|{day}|09:15:00", "#SYM|CUR|NAME|CON|ISIN|REBATERATE|FEERATE|AVAILABLE|FIGI|"]
    tail = [f"#EOF|{len(rows) if count is None else count}"]
    return "\n".join(head + rows + tail).encode()


IBKR_ROWS = [
    "AAPL|USD|APPLE INC|265598|XXXXXXX31005|3.4719|0.4081|>10000000|BBG000B9XRY4|",
    "BRK B|USD|BERKSHIRE HATHAWAY INC-CL B|72063691|XXXXXXX07026|3.6018|0.2782|900000|"
    "BBG000DWG505|",
    "XYZ|USD|AMPERSAND &amp; CO|1|US0000000001|-40.0|45.5|1000||",
]


class TestIbkrBorrow:
    def test_the_header_gives_the_eastern_date_time_and_count(self):
        d, stamp, n = ibkr_borrow.header(_ibkr_file(IBKR_ROWS))
        assert d == dt.date(2026, 10, 12)
        assert stamp.tzinfo == EASTERN and stamp.hour == 9
        assert n == 3

    def test_a_file_with_no_eof_is_truncated(self):
        with pytest.raises(AuditFailure, match="truncated"):
            ibkr_borrow.header(_ibkr_file(IBKR_ROWS)[:-10])

    def test_build_cleans_the_fields(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(ibkr_borrow, "MIN_ROWS", 1)
        src = common.write_gzip(_ibkr_file(IBKR_ROWS),
                                ibkr_borrow.vendor_file(dt.date(2026, 10, 12)))
        path = ibkr_borrow._publish_from(src)
        rows = {r[0]: r for r in duckdb.sql(f"""
            select ticker, name, isin, fee_rate, available, available_capped, composite_figi
            from read_parquet('{path}')""").fetchall()}
        assert rows["BRK.B"][4] == 900000 and rows["BRK.B"][5] is False
        assert rows["AAPL"][4] == 10000000 and rows["AAPL"][5] is True  # ">10000000" is a cap.
        assert rows["AAPL"][2] is None  # IBKR masks most ISINs.
        assert rows["XYZ"][1] == "AMPERSAND & CO" and rows["XYZ"][6] is None

    def test_a_row_count_that_disagrees_with_eof_is_fatal(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(ibkr_borrow, "MIN_ROWS", 1)
        src = common.write_gzip(_ibkr_file(IBKR_ROWS, count=5),
                                ibkr_borrow.vendor_file(dt.date(2026, 10, 12)))
        with pytest.raises(AuditFailure, match="#EOF says 5"):
            ibkr_borrow._publish_from(src)
        assert not dal.IBKR_BORROW.partition_file(dt.date(2026, 10, 12)).exists()

    def test_a_negative_fee_is_fatal(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(ibkr_borrow, "MIN_ROWS", 1)
        bad = IBKR_ROWS[:2] + ["NEG|USD|NEG|2||1.0|-0.5|10||"]
        src = common.write_gzip(_ibkr_file(bad), ibkr_borrow.vendor_file(dt.date(2026, 10, 12)))
        with pytest.raises(AuditFailure, match="negative fee"):
            ibkr_borrow._publish_from(src)

    @pytest.mark.parametrize("utc, due", [
        (dt.datetime(2026, 10, 12, 11, 30, tzinfo=dt.UTC), False),  # 07:30 ET, before the hour.
        (dt.datetime(2026, 10, 12, 13, 30, tzinfo=dt.UTC), True),
        (dt.datetime(2026, 10, 11, 15, 0, tzinfo=dt.UTC), False),   # A Sunday.
    ])
    def test_a_snapshot_is_due_on_a_session_after_the_pull_hour(self, tmp_data_root, utc, due):
        assert ibkr_borrow.due(utc) is due

    def test_a_file_dated_on_a_weekend_is_not_published(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(ibkr_borrow, "fetch", lambda: _ibkr_file(IBKR_ROWS, day="2026.10.11"))
        assert ibkr_borrow.ingest(force=True) is None
        assert dal.partitions(dal.IBKR_BORROW) == []

    def test_force_never_replaces_a_published_snapshot(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(ibkr_borrow, "MIN_ROWS", 1)
        monkeypatch.setattr(ibkr_borrow, "fetch", lambda: _ibkr_file(IBKR_ROWS))
        first = ibkr_borrow.ingest(force=True)
        before = first.stat().st_mtime_ns
        monkeypatch.setattr(ibkr_borrow, "fetch", lambda: _ibkr_file(IBKR_ROWS[:2]))
        assert ibkr_borrow.ingest(force=True) == first
        assert first.stat().st_mtime_ns == before


ISHARES_DOC = {
    "1": {"productView": ["etf"], "localExchangeTicker": "IVV",
          "fundName": "iShares Core S&P 500 ETF",
          "aladdinAssetClass": "Equity", "navAmount": {"d": "781.89", "r": 781.89},
          "navAmountAsOf": {"d": "Oct 09, 2026", "r": 20261009},
          "totalNetAssetsFund": {"d": "896", "r": 8.96e11},
          "totalNetAssetsFundAsOf": {"r": 20261009}, "inceptionDate": {"r": 20000515},
          "fees": {"r": 0.03}, "productPageUrl": "/us/products/239726/ishares-core-sp-500-etf"},
    "2": {"productView": ["mutualFund"], "localExchangeTicker": "BLK", "fundName": "Not an ETF"},
}
SSGA_DOC = {"data": {"funds": {"etfs": {
    "categories": [{"key": "assetclass", "subCategories": [
        {"name": "Equity", "funds": "SPY", "subCategories": [{"name": "Core", "funds": "SPY"}]},
        {"name": "Alternative Sector", "funds": "GLD",
         "subCategories": [{"name": "Commodities & CTAs", "funds": "GLD"}]}]}],
    "datas": [
        {"fundTicker": "SPY®", "fundName": "SPDR® S&P 500 ETF Trust", "nav": ["$773.77", 773.77],
         "aum": ["$817,200.17 M", 817200.17], "asOfDate": ["Oct 08 2026", "2026-10-08"],
         "inceptionDate": ["Jan 22 1993", "1993-01-22"], "ter": ["0.09%", 0.0945],
         "closePrice": ["$773.93", 773.93], "premiumDiscount": ["0.02%", 0.02],
         "perfIndex": [{"glossary": "S&P 500 Index"}], "fundUri": "/spy"},
        {"fundTicker": "GLD", "fundName": "SPDR Gold Shares", "nav": ["$380.00", 380.0],
         "aum": ["$100,000.00 M", 100000.0], "asOfDate": ["Oct 08 2026", "2026-10-08"],
         "premiumDiscount": ["-", -5e-324], "perfIndex": [{"glossary": "LBMA Gold Price PM"}]},
    ]}}}}


class TestEtfFunds:
    def test_ishares_keeps_etfs_and_reads_raw_values(self):
        rows = etf_funds.parse_ishares(ISHARES_DOC, dt.date(2026, 10, 11))
        assert len(rows) == 1
        r = dict(zip(etf_funds.COLUMNS, rows[0], strict=True))
        assert r["fund"] == "IVV" and r["nav"] == 781.89 and r["nav_date"] == dt.date(2026, 10, 9)

    def test_ssga_maps_classes_strips_marks_and_states_assets_in_dollars(self):
        rows = {r[2]: dict(zip(etf_funds.COLUMNS, r, strict=True))
                for r in etf_funds.parse_ssga(SSGA_DOC, dt.date(2026, 10, 11))}
        assert rows["SPY"]["asset_class"] == "Equity"
        assert rows["SPY"]["total_net_assets"] == pytest.approx(8.1720017e11)
        assert rows["SPY"]["benchmark"] == "S&P 500 Index"
        assert rows["GLD"]["asset_class"] == "Commodity"
        assert rows["GLD"]["premium_discount"] is None  # A display of "-" is no value.

    def test_one_snapshot_per_eastern_date(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(etf_funds, "MIN_ISHARES", 1)
        monkeypatch.setattr(etf_funds, "MIN_SSGA", 1)
        calls = []
        docs = {"ishares": json.dumps(ISHARES_DOC).encode(), "ssga": json.dumps(SSGA_DOC).encode()}

        def get(url, *a, **k):
            calls.append(url)
            return docs["ishares" if "ishares" in url else "ssga"]

        monkeypatch.setattr(etf_funds, "get_public", get)
        now = dt.datetime(2026, 10, 12, 2, 0, tzinfo=dt.UTC)  # 22:00 ET on the 11th.
        path = etf_funds.ingest(now)
        assert path == dal.ETF_FUNDS.partition_file(dt.date(2026, 10, 11))
        assert etf_funds.ingest(now) == path and len(calls) == 2


def _xlsx(rows: list[list]) -> bytes:
    wb = openpyxl.Workbook()
    for r in rows:
        wb.active.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


class TestEtfNavHistory:
    def test_parse_drops_the_zero_rows_of_a_launch_day(self, tmp_path):
        f = tmp_path / "XYZ.xlsx"
        f.write_bytes(_xlsx([["Fund Name:", "X"], ["Ticker Symbol:", "XYZ"], [],
                             ["Date", "NAV", "Shares Outstanding", "Total Net Assets"],
                             ["09-Oct-2026", 10.5, 1000, 10500], ["01-Jun-2006", 0, 0, 0],
                             [], ["Disclaimer text"]]))
        assert etf_nav_history.parse(f) == [(dt.date(2026, 10, 9), 10.5, 1000.0, 10500.0)]

    def test_the_fold_takes_each_day_from_the_newest_pull(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(etf_nav_history, "MIN_FUNDS", 1)
        for pull, nav in ((dt.date(2026, 9, 1), 10.0), (dt.date(2026, 10, 1), 11.0)):
            d = etf_nav_history.pull_dir(pull)
            d.mkdir(parents=True)
            (d / "XYZ.xlsx").write_bytes(_xlsx([
                ["Date", "NAV", "Shares Outstanding", "Total Net Assets"],
                ["09-Aug-2026", nav, 1, nav]]))
        path = etf_nav_history.ingest(dt.date(2026, 10, 2), rebuild=True)
        assert duckdb.sql(f"select nav, vendor_pull_date from read_parquet('{path}')").fetchall() \
            == [(11.0, dt.date(2026, 10, 1))]


ISHARES_CSV = """﻿iShares Core S&P 500 ETF
Fund Holdings as of,"Oct 08, 2026"
Shares Outstanding,"1,147,500,000.00"

Ticker,Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Quantity,Price,Location,Exchange,Currency,FX Rate,Market Currency,Accrual Date
"NVDA","NVIDIA","Information Technology","Equity","74,146,212,999.84","8.32","74,146,212,999.84","321,703,458.00","230.48","United States","NASDAQ","USD","1.00","USD","-"
"BRK B","BERKSHIRE","Financials","Equity","1,000.00","1.68","1,000.00","10.00","100.00","United States","NYSE","USD","1.00","USD","-"
"USD","USD CASH","Cash and/or Derivatives","Cash","5.00","0.00","5.00","5.00","100.00","United States","-","USD","1.00","USD","-"

"The content contained herein is owned or licensed by BlackRock"
"""  # noqa: E501


class TestEtfHoldings:
    def test_ishares_parse_reads_the_preamble_and_converts_percent(self):
        d, so, rows = etf_holdings.parse_ishares(ISHARES_CSV)
        assert d == dt.date(2026, 10, 8) and so == 1_147_500_000.0
        assert len(rows) == 3  # The footnote after the table is not a holding.
        assert rows[0]["weight"] == pytest.approx(0.0832)
        assert rows[0]["market_value"] == pytest.approx(74_146_212_999.84)

    def test_ssga_parse_reads_the_as_of_line_and_splits_identifiers(self):
        data = _xlsx([["Fund Name:", "SPDR S&P 500"], ["Ticker Symbol:", "SPY"],
                      ["Holdings:", "As of 08-Oct-2026"], [],
                      ["Name", "Ticker", "Identifier", "SEDOL", "Weight", "Sector", "Shares Held",
                       "Local Currency"],
                      ["NVIDIA CORP", "NVDA", "67066G104", "2379504", "8.33", "-", 295403453,
                       "USD"],
                      ["BOND X", None, "US68288AAA51", "-", "0.5", "-", 100, "USD"],
                      [], ["Bonds generally present less short-term risk"]])
        d, _, rows = etf_holdings.parse_ssga(data)
        assert d == dt.date(2026, 10, 8) and len(rows) == 2
        assert rows[0]["cusip"] == "67066G104" and rows[1]["isin"] == "US68288AAA51"
        assert rows[0]["sector"] is None and rows[0]["weight"] == pytest.approx(0.0833)

    def test_overlap_keys_stocks_by_ticker_and_bonds_by_isin(self):
        a = etf_holdings.weights([{"ticker": "BRK B", "weight": 0.5},
                                  {"ticker": "AAPL", "weight": 0.5},
                                  {"ticker": "USD", "weight": 0.1, "asset_class": "Cash"}])
        b = etf_holdings.weights([{"ticker": "BRK.B", "cusip": "084670702", "weight": 0.25},
                                  {"ticker": "MSFT", "weight": 0.75}])
        assert set(a) == {"T:BRK.B", "T:AAPL"}  # Cash is not a holding.
        assert etf_holdings.overlap(a, b) == pytest.approx(0.25)
        bonds = etf_holdings.weights([{"isin": "US1", "cusip": "C1", "weight": 1.0}])
        assert set(bonds) == {"US1"}

    def test_a_file_publishes_once_under_its_as_of_date(self, tmp_data_root):
        today = dt.date(2026, 10, 12)
        status, d = etf_holdings._publish("IVV", "iShares", ISHARES_CSV.encode(), today)
        assert (status, d) == ("published", dt.date(2026, 10, 8))
        again = etf_holdings._publish("IVV", "iShares", ISHARES_CSV.encode(), today)
        assert again[0] == "unchanged"
        assert etf_holdings.newest_dates() == {"IVV": dt.date(2026, 10, 8)}
        assert dal.etf_holdings().filter("fund = 'IVV'").aggregate("count(*)").fetchone() == (3,)

    def test_a_file_dated_after_today_is_fatal(self, tmp_data_root):
        with pytest.raises(AuditFailure, match="after today"):
            etf_holdings._publish("IVV", "iShares", ISHARES_CSV.encode(), dt.date(2026, 10, 1))

    def test_funds_are_due_until_current_and_throttled_after_a_try(self, tmp_data_root,
                                                                  monkeypatch):
        monkeypatch.setattr(etf_holdings, "FUNDS_FILE", tmp_data_root / "funds.csv")
        etf_holdings.write_funds([{"fund": "IVV", "issuer": "iShares", "holdings_url": "u"},
                                  {"fund": "SPY", "issuer": "SSGA", "holdings_url": "v"}])
        etf_holdings._publish("IVV", "iShares", ISHARES_CSV.encode(), dt.date(2026, 10, 12))
        now = dt.datetime(2026, 10, 9, 14, 0, tzinfo=dt.UTC)  # The last session is 10-08.
        assert [f["fund"] for f in etf_holdings.due_funds(now)] == ["SPY"]
        etf_holdings._write_state({"SPY": common.eastern(now).isoformat()})
        assert etf_holdings.due_funds(now + dt.timedelta(hours=1)) == []
        assert [f["fund"] for f in etf_holdings.due_funds(now + dt.timedelta(hours=9))] == ["SPY"]

    def test_vendor_thinning_keeps_recent_dates_and_month_ends(self, tmp_data_root):
        root = tmp_data_root / "vendor" / "etf_holdings"
        for d in ("2026-07-30", "2026-07-31", "2026-08-03", "2026-10-01"):
            (root / d).mkdir(parents=True)
        etf_holdings.thin_vendor(dt.date(2026, 10, 12))
        assert sorted(p.name for p in root.iterdir()) == ["2026-07-31", "2026-08-03",
                                                          "2026-10-01"]


class TestPerMemberPartitions:
    def test_a_partition_with_one_file_per_fund_reads_as_one(self, tmp_data_root):
        d = dt.date(2026, 10, 8)
        for fund in ("IVV", "IWM"):
            path = dal.ETF_HOLDINGS.member_file(d, fund)
            common.rows_to_parquet([(d, fund, 0.5)], {"date": "date", "fund": "varchar",
                                                      "weight": "double"}, path)
        assert dal.partitions(dal.ETF_HOLDINGS) == [d]
        assert sorted(r[0] for r in dal.on_date(dal.ETF_HOLDINGS, d).project("fund").fetchall()) \
            == ["IVV", "IWM"]


def test_rows_to_parquet_keeps_nulls_dates_and_commas(tmp_path):
    path = tmp_path / "x.parquet"
    n = common.rows_to_parquet([(dt.date(2026, 1, 2), 'a, "b"', None), (None, None, 1.5)],
                               {"d": "date", "s": "varchar", "x": "double"}, path)
    assert n == 2
    assert duckdb.sql(f"select * from read_parquet('{path}')").fetchall() == [
        (dt.date(2026, 1, 2), 'a, "b"', None), (None, None, 1.5)]


def test_vendor_gzip_round_trip(tmp_path):
    p = common.write_gzip(b"abc", tmp_path / "f.gz")
    assert gzip.decompress(p.read_bytes()) == b"abc"

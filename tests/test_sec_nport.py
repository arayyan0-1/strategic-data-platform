"""Tests for the N-PORT ingest and the OpenFIGI CUSIP mapping. No network."""
import datetime as dt
import gzip
import json
import logging
import re
import time
from pathlib import Path
from types import SimpleNamespace

import duckdb
import httpx
import pytest

from sdp import dal
from sdp.config import settings
from sdp.ingest import openfigi, sec_nport
from sdp.ingest.common import AuditFailure

FIX = Path(__file__).parent / "fixtures"
CONTACT = "Test Suite test@example.invalid"


@pytest.fixture
def sleeps(monkeypatch):
    """Replace the sleep of both ingests with a recorder; the clock stays real."""
    calls: list[float] = []
    fake = SimpleNamespace(monotonic=time.monotonic, sleep=calls.append)
    monkeypatch.setattr(sec_nport, "time", fake)
    monkeypatch.setattr(openfigi, "time", fake)
    return calls


def _doc(series="S000000001", rep="2026-06-30", lines=None) -> bytes:
    """A minimal primary_doc.xml. A line is (cusip, name, asset_cat, payoff, pct_val)."""
    lines = lines if lines is not None else [
        ("037833100", "Apple Inc.", "EC", "Long", 60.0),
        ("594918104", "Microsoft Corp.", "EC", "Long", 30.0),
        ("000000000", "Foreign Co.", "EC", "Long", 9.0),
        ("N/A", "E-Mini future", "DE", "N/A", 1.0),
    ]
    body = "".join(f"""
      <invstOrSec><name>{n}</name><title>{n}</title><cusip>{c}</cusip>
        <identifiers><isin value="US{c}0"/></identifiers>
        <balance>10</balance><units>NS</units><curCd>USD</curCd><valUSD>{p * 100}</valUSD>
        <pctVal>{p}</pctVal><payoffProfile>{pay}</payoffProfile><assetCat>{cat}</assetCat>
        <issuerCat>CORP</issuerCat><invCountry>US</invCountry></invstOrSec>"""
                   for c, n, cat, pay, p in lines)
    series_tag = f"<seriesId>{series}</seriesId>" if series else ""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/nport" xmlns:com="http://www.sec.gov/edgar/common">
  <formData><genInfo>{series_tag}<repPdDate>{rep}</repPdDate></genInfo>
    <invstOrSecs>{body}</invstOrSecs></formData></edgarSubmission>""".encode()


def _atom(entries: list[tuple[str, str, str]]) -> bytes:
    """An Atom page from (accession, filed, form)."""
    base = "https://www.sec.gov/Archives/edgar/data/1100663"
    items = "".join(f"""
      <entry><content type="text/xml"><accession-number>{a}</accession-number>
        <filing-date>{d}</filing-date><filing-type>{f}</filing-type>
        <filing-href>{base}/{a.replace("-", "")}/{a}-index.htm</filing-href>
      </content></entry>""" for a, d, f in entries)
    return (f'<?xml version="1.0" encoding="ISO-8859-1" ?>'
            f'<feed xmlns="http://www.w3.org/2005/Atom">{items}</feed>').encode()


class Edgar:
    """A fake EDGAR: filings are {accession: (filed, form, document)}."""

    def __init__(self, filings):
        self.filings = filings
        self.cap = 100
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path == "/cgi-bin/browse-edgar":
            q = request.url.params
            newest = sorted(((a, f.isoformat() if hasattr(f, "isoformat") else f, form)
                             for a, (f, form, _) in self.filings.items()),
                            key=lambda e: e[1], reverse=True)
            start, count = int(q["start"]), min(int(q["count"]), self.cap)
            return httpx.Response(200, content=_atom(newest[start:start + count]))
        m = re.search(r"/data/(\d+)/(\d{10})(\d\d)(\d{6})/primary_doc\.xml$", request.url.path)
        if not m:
            return httpx.Response(404)
        acc = f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
        if acc not in self.filings:
            return httpx.Response(404)
        return httpx.Response(200, content=self.filings[acc][2])

    @property
    def documents(self) -> list[str]:
        return [r.url.path for r in self.requests if r.url.path.endswith("primary_doc.xml")]

    @property
    def lists(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path == "/cgi-bin/browse-edgar"]


@pytest.fixture
def seed(tmp_data_root, tmp_path, monkeypatch, sleeps):
    """One fund TST in series S000000001, and a contact for the client."""
    f = tmp_path / "nport_funds.csv"
    f.write_text("fund,cik,series_id\nTST,1100663,S000000001\n", encoding="utf-8")
    monkeypatch.setattr(sec_nport, "FUNDS_FILE", f)
    monkeypatch.setattr(settings, "sec_user_agent", CONTACT)
    return f


def _client(edgar: Edgar) -> sec_nport.Sec:
    return sec_nport.Sec(transport=httpx.MockTransport(edgar), interval=0)


def _published(fund="TST", d=dt.date(2026, 6, 30)) -> list[tuple]:
    path = dal.SEC_NPORT.member_file(d, fund)
    return duckdb.execute(f"select * from read_parquet('{path}') order by cusip").fetchall()


FILINGS = {
    "0000000001-26-000001": (dt.date(2026, 8, 25), "NPORT-P", _doc()),
}


# ---------- parsing ----------

class TestParseFilings:
    def test_the_ivv_extract(self):
        p = sec_nport.parse_filing((FIX / "nport_ivv.xml").read_bytes())
        assert p.series_id == "S000004310" and p.report_date == dt.date(2026, 6, 30)
        assert len(p.lines) == 8
        assert sum(line[7] for line in p.lines) == pytest.approx(0.775358240509 / 100)
        first = dict(zip(sec_nport.LINE_FIELDS, p.lines[0], strict=True))
        assert first == {
            "cusip": "12504L109", "isin": "US12504L1098", "name": "CBRE Group, Inc.",
            "title": "CBRE Group, Inc., Class A", "balance": 4027521.0, "units": "NS",
            "val_usd": 542466803.49, "pct_val": pytest.approx(0.061079735228 / 100),
            "asset_cat": "EC", "issuer_cat": "CORP", "inv_country": "US", "payoff": "Long"}

    def test_the_vti_extract(self):
        p = sec_nport.parse_filing((FIX / "nport_vti.xml").read_bytes())
        assert p.series_id == "S000002848" and p.report_date == dt.date(2026, 6, 30)
        assert len(p.lines) == 8
        assert sum(line[7] for line in p.lines) == pytest.approx(0.276054736726 / 100)

    def test_a_placeholder_is_no_identifier_and_a_missing_field_is_null(self):
        p = sec_nport.parse_filing((FIX / "nport_ivv.xml").read_bytes())
        by_name = {line[2]: dict(zip(sec_nport.LINE_FIELDS, line, strict=True))
                   for line in p.lines}
        future = by_name["N/A"]  # An index future: no CUSIP, no ISIN, a payoff of N/A.
        assert future["cusip"] is None and future["isin"] is None
        assert future["payoff"] == "N/A" and future["asset_cat"] == "DE"
        assert by_name["Hologic, Inc."]["asset_cat"] is None  # The filer left it out.

    def test_a_document_without_a_series_or_date_parses_to_none(self):
        p = sec_nport.parse_filing(_doc(series=None))
        assert p.series_id is None
        assert len(p.lines) == 4 and p.lines[2][0] is None  # 000000000 is a placeholder.


class TestParseAtom:
    def test_the_ivv_list(self):
        filings = sec_nport.parse_atom((FIX / "ivv_series.atom").read_bytes())
        assert [(f.accession, f.filed.isoformat(), f.form) for f in filings] == [
            ("0002071691-26-019760", "2026-08-25", "NPORT-P"),
            ("0002071691-26-015790", "2026-07-13", "NPORT-P/A"),
            ("0002071691-26-012459", "2026-05-28", "NPORT-P"),
            ("0002071691-26-004238", "2026-02-25", "NPORT-P"),
        ]
        assert filings[0].cik == "1100663"
        assert filings[0].url == (f"{settings.sec_www_url}/Archives/edgar/data/1100663/"
                                  "000207169126019760/primary_doc.xml")

    def test_paging_follows_start_until_an_empty_page(self, monkeypatch, sleeps):
        monkeypatch.setattr(settings, "sec_user_agent", CONTACT)
        monkeypatch.setattr(sec_nport, "ATOM_COUNT", 2)
        edgar = Edgar({f"0000000001-26-00000{i}": (dt.date(2026, i, 1), "NPORT-P", b"")
                       for i in range(1, 6)})
        found = sec_nport.list_filings(_client(edgar), "S000000001", dt.date(2021, 1, 1))
        assert [r.url.params["start"] for r in edgar.lists] == ["0", "2", "4", "5"]
        assert edgar.lists[0].url.params["CIK"] == "S000000001"
        assert edgar.lists[0].url.params["type"] == "NPORT-P"
        assert [f.accession[-1] for f in found] == ["1", "2", "3", "4", "5"]

    def test_a_server_that_caps_the_page_is_still_read_in_full(self, monkeypatch, sleeps):
        monkeypatch.setattr(settings, "sec_user_agent", CONTACT)
        edgar = Edgar({f"0000000001-26-00000{i}": (dt.date(2026, i, 1), "NPORT-P", b"")
                       for i in range(1, 6)})
        edgar.cap = 2  # It answers 2 entries to a request for 100.
        found = sec_nport.list_filings(_client(edgar), "S000000001", dt.date(2021, 1, 1))
        assert len(found) == 5

    def test_paging_stops_at_a_page_older_than_since_and_drops_amendments(self, monkeypatch,
                                                                         sleeps):
        monkeypatch.setattr(settings, "sec_user_agent", CONTACT)
        monkeypatch.setattr(sec_nport, "ATOM_COUNT", 2)
        edgar = Edgar({
            "0000000001-26-000005": (dt.date(2026, 5, 1), "NPORT-P", b""),
            "0000000001-26-000004": (dt.date(2026, 4, 1), "NPORT-P/A", b""),
            "0000000001-26-000003": (dt.date(2026, 3, 1), "NPORT-P", b""),
            "0000000001-26-000002": (dt.date(2026, 2, 1), "NPORT-P", b""),
            "0000000001-26-000001": (dt.date(2026, 1, 1), "NPORT-P", b""),
        })
        found = sec_nport.list_filings(_client(edgar), "S000000001", dt.date(2026, 3, 1))
        assert len(edgar.lists) == 2  # The second page reaches February, so it is the last.
        assert [f.accession[-1] for f in found] == ["3", "5"]


# ---------- the client ----------

class TestClient:
    @pytest.mark.parametrize("agent", [None, "", "  "])
    def test_an_unset_contact_is_a_clear_error(self, monkeypatch, agent):
        monkeypatch.setattr(settings, "sec_user_agent", agent)
        with pytest.raises(sec_nport.MissingContact) as err:
            sec_nport.Sec()
        assert str(err.value) == ("Set SEC_USER_AGENT in .env to 'Name email'. "
                                  "SEC refuses a request without a contact.")

    def test_ingest_stops_before_any_work_without_a_contact(self, seed, monkeypatch):
        monkeypatch.setattr(settings, "sec_user_agent", None)
        with pytest.raises(sec_nport.MissingContact):
            sec_nport.ingest()

    def test_every_request_sends_the_contact(self, seed):
        edgar = Edgar(dict(FILINGS))
        assert sec_nport.ingest(sec=_client(edgar)) == []
        assert edgar.requests
        assert {r.headers["user-agent"] for r in edgar.requests} == {CONTACT}

    def test_requests_are_spaced(self, monkeypatch):
        now = [100.0]
        sleeps: list[float] = []

        def sleep(s):
            sleeps.append(s)
            now[0] += s

        monkeypatch.setattr(sec_nport, "time", SimpleNamespace(
            monotonic=lambda: now[0], sleep=sleep))
        t = sec_nport.Throttle(0.2)
        for _ in range(3):
            t.wait()
        assert sleeps == pytest.approx([0.2, 0.2])
        assert sec_nport.MIN_INTERVAL >= 0.2  # At most 5 requests a second.

    def _sec(self, *steps):
        queue = list(steps)
        seen = []

        def handler(request):
            seen.append(request)
            step = queue.pop(0)
            if isinstance(step, type) and issubclass(step, Exception):
                raise step("simulated", request=request)
            return step

        return sec_nport.Sec(transport=httpx.MockTransport(handler), interval=0), seen

    def test_503_429_and_a_transport_error_retry(self, monkeypatch, sleeps):
        monkeypatch.setattr(settings, "sec_user_agent", CONTACT)
        sec, seen = self._sec(
            httpx.Response(503), httpx.Response(429, headers={"Retry-After": "7"}),
            httpx.ReadTimeout, httpx.Response(200, content=b"ok"))
        assert sec.get("https://sec.test/x") == b"ok"
        assert len(seen) == 4 and sleeps == [1, 7, 4]

    def test_a_404_stops_at_once(self, monkeypatch, sleeps):
        monkeypatch.setattr(settings, "sec_user_agent", CONTACT)
        sec, seen = self._sec(httpx.Response(404))
        with pytest.raises(RuntimeError, match="HTTP 404"):
            sec.get("https://sec.test/x")
        assert len(seen) == 1 and sleeps == []

    def test_a_403_names_the_likely_cause_without_the_contact(self, monkeypatch, sleeps):
        monkeypatch.setattr(settings, "sec_user_agent", CONTACT)
        sec, _ = self._sec(httpx.Response(403))
        with pytest.raises(RuntimeError, match="User-Agent") as err:
            sec.get("https://sec.test/x")
        assert CONTACT not in str(err.value)

    def test_it_gives_up_after_the_attempts(self, monkeypatch, sleeps):
        monkeypatch.setattr(settings, "sec_user_agent", CONTACT)
        sec, seen = self._sec(*[httpx.Response(500)] * 3)
        with pytest.raises(RuntimeError, match="All 3 attempts"):
            sec.get("https://sec.test/x", attempts=3)
        assert len(seen) == 3


# ---------- write, audit, publish ----------

class TestIngest:
    def test_a_filing_is_stored_gzipped_and_published_under_its_report_date(self, seed):
        edgar = Edgar(dict(FILINGS))
        assert sec_nport.ingest(sec=_client(edgar)) == []
        vf = sec_nport.vendor_file("S000000001", "0000000001-26-000001")
        assert gzip.decompress(vf.read_bytes()) == _doc()
        assert sec_nport.read_index("S000000001") == {
            "0000000001-26-000001": {"filed": "2026-08-25", "report_date": "2026-06-30"}}
        rows = duckdb.execute(f"""
            select fund, series_id, accession, filed, report_date, cusip, isin, name, balance,
                   units, val_usd, pct_val, asset_cat, issuer_cat, inv_country, payoff
            from read_parquet('{dal.SEC_NPORT.member_file(dt.date(2026, 6, 30), "TST")}')
            order by pct_val desc""").fetchall()
        assert len(rows) == 4
        assert rows[0] == ("TST", "S000000001", "0000000001-26-000001", dt.date(2026, 8, 25),
                           dt.date(2026, 6, 30), "037833100", "US0378331000", "Apple Inc.", 10.0,
                           "NS", 6000.0, pytest.approx(0.6), "EC", "CORP", "US", "Long")
        assert dal.partitions(dal.SEC_NPORT) == [dt.date(2026, 6, 30)]
        assert dal.sec_nport().aggregate("sum(pct_val)").fetchone()[0] == pytest.approx(1.0)

    def test_a_second_run_downloads_no_document_and_changes_no_file(self, seed):
        edgar = Edgar(dict(FILINGS))
        sec_nport.ingest(sec=_client(edgar))
        path = dal.SEC_NPORT.member_file(dt.date(2026, 6, 30), "TST")
        before = path.stat().st_mtime_ns
        edgar.requests.clear()
        assert sec_nport.ingest(sec=_client(edgar)) == []
        assert edgar.documents == [] and len(edgar.lists) == 2  # A page, then the empty one.
        assert path.stat().st_mtime_ns == before

    def test_two_filings_for_one_date_keep_the_earlier_one(self, seed):
        edgar = Edgar({
            "0000000001-26-000009": (dt.date(2026, 9, 10), "NPORT-P", _doc(
                lines=[("037833100", "Apple Inc.", "EC", "Long", 100.0)])),
            "0000000001-26-000004": (dt.date(2026, 8, 25), "NPORT-P", _doc()),
            "0000000001-26-000005": (dt.date(2026, 9, 1), "NPORT-P/A", _doc()),
        })
        assert sec_nport.ingest(sec=_client(edgar)) == []
        rows = _published()
        assert {r[2] for r in rows} == {"0000000001-26-000004"}
        assert {r[3] for r in rows} == {dt.date(2026, 8, 25)}
        assert len(rows) == 4
        assert not any("000000000126000005" in p for p in edgar.documents)  # No amendment fetched.

    def test_a_later_original_for_a_published_date_changes_nothing(self, seed):
        first = Edgar({"0000000001-26-000004": (dt.date(2026, 8, 25), "NPORT-P", _doc())})
        sec_nport.ingest(sec=_client(first))
        both = Edgar({
            "0000000001-26-000004": (dt.date(2026, 8, 25), "NPORT-P", _doc()),
            "0000000001-26-000009": (dt.date(2026, 9, 10), "NPORT-P", _doc(
                lines=[("037833100", "Apple Inc.", "EC", "Long", 100.0)])),
        })
        assert sec_nport.ingest(sec=_client(both)) == []
        assert {r[2] for r in _published()} == {"0000000001-26-000004"}

    def test_since_leaves_earlier_filings_out(self, seed):
        edgar = Edgar({
            "0000000001-22-000001": (dt.date(2022, 2, 1), "NPORT-P", _doc(rep="2021-12-31")),
            "0000000001-26-000001": (dt.date(2026, 8, 25), "NPORT-P", _doc()),
        })
        sec_nport.ingest(since=dt.date(2023, 1, 1), sec=_client(edgar))
        assert dal.partitions(dal.SEC_NPORT) == [dt.date(2026, 6, 30)]

    @pytest.mark.parametrize("doc, message", [
        (_doc(series="S000009999"), "another series"),
        (_doc(lines=[("037833100", "Apple Inc.", "EC", "Long", 20.0)]), "sums to 0.200"),
        (_doc(lines=[("037833100", "Apple Inc.", "EC", "Long", 80.0),
                     ("594918104", "Microsoft Corp.", "EC", "Long", 80.0)]), "sums to 1.600"),
        (_doc(lines=[]), "no holdings"),
    ])
    def test_a_failed_audit_publishes_nothing_but_keeps_the_vendor_file(self, seed, doc, message):
        edgar = Edgar({"0000000001-26-000001": (dt.date(2026, 8, 25), "NPORT-P", doc)})
        problems = sec_nport.ingest(sec=_client(edgar))
        assert len(problems) == 1 and message in problems[0]
        assert dal.partitions(dal.SEC_NPORT) == []
        assert sec_nport.vendor_file("S000000001", "0000000001-26-000001").exists()
        assert list((settings.staging_dir / "sec_nport").rglob("*.parquet")) == []

    def test_a_filing_dated_before_its_report_is_fatal(self, seed):
        edgar = Edgar({"0000000001-26-000001": (dt.date(2026, 6, 1), "NPORT-P", _doc())})
        problems = sec_nport.ingest(sec=_client(edgar))
        assert "before the report date" in problems[0]

    def test_a_document_without_a_report_date_is_not_indexed(self, seed):
        bad = _doc().replace(b"<repPdDate>2026-06-30</repPdDate>", b"")
        edgar = Edgar({"0000000001-26-000001": (dt.date(2026, 8, 25), "NPORT-P", bad)})
        problems = sec_nport.ingest(sec=_client(edgar))
        assert "no report date" in problems[0]
        assert sec_nport.read_index("S000000001") == {}

    def test_fewer_than_ten_holdings_warn_and_still_publish(self, seed, caplog):
        edgar = Edgar(dict(FILINGS))
        with caplog.at_level(logging.WARNING):
            assert sec_nport.ingest(sec=_client(edgar)) == []
        assert "only 4 holdings" in caplog.text
        assert len(_published()) == 4

    def test_one_failing_filing_does_not_stop_the_others(self, seed):
        edgar = Edgar({
            "0000000001-26-000001": (dt.date(2026, 8, 25), "NPORT-P", _doc()),
            "0000000001-25-000001": (dt.date(2025, 8, 25), "NPORT-P", b"<broken"),
        })
        problems = sec_nport.ingest(sec=_client(edgar))
        assert len(problems) == 1 and "0000000001-25-000001" in problems[0]
        assert dal.partitions(dal.SEC_NPORT) == [dt.date(2026, 6, 30)]

    def test_an_unknown_fund_is_refused(self, seed):
        with pytest.raises(ValueError, match="Not in nport_funds.csv: NOPE"):
            sec_nport.ingest(["NOPE"])

    def test_funds_listed_recently_are_skipped_when_min_age_is_set(self, seed):
        now = dt.datetime(2026, 10, 13, 15, 0, tzinfo=dt.UTC)
        edgar = Edgar(dict(FILINGS))
        sec_nport.ingest(sec=_client(edgar), now=now)
        n = len(edgar.requests)
        later = now + dt.timedelta(hours=5)
        sec_nport.ingest(sec=_client(edgar), min_age_hours=20, now=later)
        assert len(edgar.requests) == n
        sec_nport.ingest(sec=_client(edgar), min_age_hours=20, now=now + dt.timedelta(hours=21))
        assert len(edgar.requests) == n + 2

    def test_two_funds_of_one_series_share_the_vendor_files(self, seed):
        seed.write_text("fund,cik,series_id\nTST,1100663,S000000001\nTS2,1100663,S000000001\n",
                        encoding="utf-8")
        edgar = Edgar(dict(FILINGS))
        assert sec_nport.ingest(sec=_client(edgar)) == []
        assert len(edgar.documents) == 1
        assert len(_published("TST")) == len(_published("TS2")) == 4


class TestRebuild:
    def test_it_republishes_from_vendor_with_no_network_and_no_contact(self, seed, monkeypatch):
        edgar = Edgar(dict(FILINGS))
        sec_nport.ingest(sec=_client(edgar))
        before = _published()
        path = dal.SEC_NPORT.member_file(dt.date(2026, 6, 30), "TST")
        path.unlink()
        monkeypatch.setattr(settings, "sec_user_agent", None)

        def no_network(*a, **k):
            raise AssertionError("A rebuild must not download.")

        monkeypatch.setattr(httpx.Client, "send", no_network)
        assert sec_nport.ingest(rebuild=True) == []
        assert _published() == before

    def test_it_replaces_a_published_file(self, seed):
        sec_nport.ingest(sec=_client(Edgar(dict(FILINGS))))
        path = dal.SEC_NPORT.member_file(dt.date(2026, 6, 30), "TST")
        path.write_bytes(b"not parquet")
        assert sec_nport.ingest(rebuild=True) == []
        assert len(_published()) == 4

    def test_a_vendor_file_with_no_filing_date_is_not_published(self, seed):
        vf = sec_nport.vendor_file("S000000001", "0000000001-26-000001")
        vf.parent.mkdir(parents=True)
        vf.write_bytes(gzip.compress(_doc()))
        assert sec_nport.ingest(rebuild=True) == []
        assert dal.partitions(dal.SEC_NPORT) == []

    def test_the_cli_refuses_to_download_without_a_contact(self, seed, monkeypatch):
        monkeypatch.setattr(settings, "sec_user_agent", None)
        with pytest.raises(SystemExit, match="Set SEC_USER_AGENT in .env"):
            sec_nport.main(["--funds", "TST"])

    def test_the_cli_rebuilds(self, seed, capsys):
        sec_nport.ingest(sec=_client(Edgar(dict(FILINGS))))
        with pytest.raises(SystemExit) as stop:
            sec_nport.main(["--funds", "TST", "--rebuild"])
        assert stop.value.code == 0 and "ok" in capsys.readouterr().out


class TestAudit:
    def test_the_audit_names_every_defect(self, tmp_path):
        staged = tmp_path / "s.parquet"
        duckdb.execute(f"""
            copy (select 'S000000009' as series_id, cast(null as date) as report_date,
                         date '2026-01-01' as filed, 2.0 as pct_val)
            to '{staged}' (format parquet)""")
        with pytest.raises(AuditFailure) as err:
            sec_nport.audit(staged, "TST", "S000000001", dt.date(2026, 6, 30))
        text = str(err.value)
        for part in ("another series", "null report date", "other than 2026-06-30",
                     "sums to 2.000"):
            assert part in text


# ---------- OpenFIGI ----------

def _listing(exch, figi, share, ticker="AAPL", typ="Common Stock"):
    return {"figi": figi, "name": "APPLE INC", "ticker": ticker, "exchCode": exch,
            "compositeFIGI": "BBG000B9XRY4", "securityType": typ, "marketSector": "Equity",
            "shareClassFIGI": share}


class FigiServer:
    """A fake OpenFIGI: known CUSIPs answer with listings, others with a warning."""

    def __init__(self, known: dict[str, list[dict]]):
        self.known = known
        self.requests: list[list[dict]] = []
        self.statuses: list[httpx.Response] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        jobs = json.loads(request.content)
        self.requests.append(jobs)
        if self.statuses:
            return self.statuses.pop(0)
        if len(jobs) > 10:
            return httpx.Response(413, text="Request may only contain 10 mapping jobs.")
        out = [({"data": self.known[j["idValue"]]} if j["idValue"] in self.known
                else {"warning": "No identifier found."}) for j in jobs]
        return httpx.Response(200, json=out, headers={
            "ratelimit-limit": "25", "ratelimit-remaining": "24", "ratelimit-reset": "60"})


def _figi(server: FigiServer) -> openfigi.OpenFigi:
    return openfigi.OpenFigi(transport=httpx.MockTransport(server), interval=0)


def _nport_lake(cusips: list[str], asset_cat="EC"):
    """Publish one sec_nport file whose lines carry these CUSIPs."""
    path = dal.SEC_NPORT.member_file(dt.date(2026, 6, 30), "TST")
    path.parent.mkdir(parents=True, exist_ok=True)
    values = ", ".join(f"('{c}', '{asset_cat}')" for c in cusips)
    duckdb.execute(f"copy (select * from (values {values}) as t(cusip, asset_cat)) "
                   f"to '{path}' (format parquet)")


def _table() -> dict[str, tuple]:
    rows = duckdb.execute(f"select * from read_parquet('{dal.OPENFIGI_CUSIPS.table_file}')"
                          ).fetchall()
    return {r[0]: r for r in rows}


class TestOpenFigi:
    def test_the_us_row_is_chosen_among_the_listings(self):
        item = {"data": [_listing("GR", "BBG1", "BBGSC"), _listing("UN", "BBG2", "BBGSC"),
                         _listing("US", "BBG3", "BBGSC", typ="Preferred"),
                         _listing("US", "BBG4", "BBGSC9", typ="Common Stock")]}
        row = openfigi.parse_result("037833100", item, dt.date(2026, 10, 13))
        assert row == ("037833100", "BBGSC9", "BBG000B9XRY4", "AAPL", "APPLE INC",
                       "Common Stock", dt.date(2026, 10, 13))

    @pytest.mark.parametrize("item", [
        {"warning": "No identifier found."}, {"error": "Invalid idValue format"},
        {"data": [_listing("GR", "BBG1", "BBGSC")]}, {"data": []},
    ])
    def test_no_match_gives_null_figis(self, item):
        assert openfigi.parse_result("X", item, dt.date(2026, 10, 13)) == (
            "X", None, None, None, None, None, dt.date(2026, 10, 13))

    def test_a_request_asks_for_the_us_composite_and_at_most_ten_jobs(self, sleeps):
        server = FigiServer({})
        client = _figi(server)
        client.map(["A", "B"])
        assert server.requests == [[{"idType": "ID_CUSIP", "idValue": "A", "exchCode": "US"},
                                    {"idType": "ID_CUSIP", "idValue": "B", "exchCode": "US"}]]
        with pytest.raises(ValueError, match="at most 10"):
            client.map([str(i) for i in range(11)])

    def test_a_429_waits_for_the_stated_delay(self, sleeps):
        server = FigiServer({})
        server.statuses = [httpx.Response(429, headers={"Retry-After": "13"})]
        assert _figi(server).map(["A"]) == [{"warning": "No identifier found."}]
        assert 13 in sleeps and len(server.requests) == 2

    def test_a_429_without_retry_after_waits_for_the_window_reset(self, sleeps):
        server = FigiServer({})
        server.statuses = [httpx.Response(429, headers={"ratelimit-reset": "42"})]
        _figi(server).map(["A"])
        assert 43 in sleeps

    def test_an_exhausted_window_delays_the_next_request(self, sleeps):
        server = FigiServer({})
        server.statuses = [httpx.Response(200, json=[{"warning": "x"}], headers={
            "ratelimit-remaining": "0", "ratelimit-reset": "30"})]
        client = openfigi.OpenFigi(transport=httpx.MockTransport(server), interval=0)
        client.map(["A"])
        client.map(["B"])
        assert any(abs(s - 31) < 1 for s in sleeps)

    def test_a_short_answer_is_an_error(self, sleeps):
        server = FigiServer({})
        server.statuses = [httpx.Response(200, json=[])]
        with pytest.raises(RuntimeError, match="answered 0 jobs for 1"):
            _figi(server).map(["A"])


class TestMapCusips:
    KNOWN = {"037833100": [_listing("US", "BBG3", "BBGSC1")],
             "594918104": [_listing("US", "BBG4", "BBGSC2", ticker="MSFT")]}

    def test_it_maps_equity_cusips_in_batches_of_ten(self, tmp_data_root, sleeps):
        cusips = [f"{i:09d}" for i in range(1, 24)] + list(self.KNOWN)
        _nport_lake(cusips)
        server = FigiServer(self.KNOWN)
        assert openfigi.ingest(client=_figi(server), today=dt.date(2026, 10, 13)) == 25
        assert [len(r) for r in server.requests] == [10, 10, 5]
        table = _table()
        assert len(table) == 25
        assert table["037833100"][1:4] == ("BBGSC1", "BBG000B9XRY4", "AAPL")
        assert table["000000001"][1:] == (None, None, None, None, None, dt.date(2026, 10, 13))
        assert dal.openfigi_cusips().aggregate("count(*)").fetchone()[0] == 25

    def test_only_equity_lines_with_a_nine_character_cusip_are_asked(self, tmp_data_root, sleeps):
        _nport_lake(["037833100", "SHORT", "N/A00"], asset_cat="EC")
        _nport_lake(["999999999"], asset_cat="DBT")  # Same file name: replaces the first.
        server = FigiServer(self.KNOWN)
        assert openfigi.ingest(client=_figi(server)) == 0
        assert server.requests == []

    def test_a_mapped_cusip_is_never_asked_again(self, tmp_data_root, sleeps):
        _nport_lake(["037833100"])
        server = FigiServer(self.KNOWN)
        openfigi.ingest(client=_figi(server), today=dt.date(2026, 10, 13))
        server.requests.clear()
        assert openfigi.ingest(client=_figi(server), today=dt.date(2027, 10, 13)) == 0
        assert server.requests == []

    def test_a_cusip_with_no_result_waits_thirty_days(self, tmp_data_root, sleeps):
        _nport_lake(["111111111"])
        server = FigiServer({})
        openfigi.ingest(client=_figi(server), today=dt.date(2026, 10, 13))
        assert _table()["111111111"][1] is None
        server.requests.clear()
        assert openfigi.ingest(client=_figi(server), today=dt.date(2026, 11, 11)) == 0
        assert openfigi.ingest(client=_figi(server), today=dt.date(2026, 11, 12)) == 1
        assert len(server.requests) == 1

    def test_new_cusips_join_the_old_table(self, tmp_data_root, sleeps):
        _nport_lake(["037833100"])
        server = FigiServer(self.KNOWN)
        openfigi.ingest(client=_figi(server), today=dt.date(2026, 10, 13))
        _nport_lake(["037833100", "594918104"])
        assert openfigi.ingest(client=_figi(server), today=dt.date(2026, 10, 14)) == 1
        assert set(_table()) == set(self.KNOWN)
        assert _table()["037833100"][6] == dt.date(2026, 10, 13)

    def test_an_interrupted_run_keeps_what_it_mapped(self, tmp_data_root, sleeps, monkeypatch):
        monkeypatch.setattr(openfigi, "CHECKPOINT", 1)
        _nport_lake([f"{i:09d}" for i in range(1, 31)])
        server = FigiServer({})
        client = _figi(server)
        real = client.map
        calls = []

        def flaky(cusips, **k):
            calls.append(cusips)
            if len(calls) == 3:
                raise RuntimeError("down")
            return real(cusips, **k)

        client.map = flaky
        with pytest.raises(RuntimeError, match="down"):
            openfigi.ingest(client=client, today=dt.date(2026, 10, 13))
        assert len(_table()) == 20

    def test_with_no_nport_file_there_is_nothing_to_do(self, tmp_data_root):
        assert openfigi.ingest(client=_figi(FigiServer({}))) == 0
        assert not dal.OPENFIGI_CUSIPS.table_file.exists()

    def test_the_audit_refuses_a_duplicate_cusip(self, tmp_path):
        staged = tmp_path / "s.parquet"
        duckdb.execute(f"""
            copy (select * from (values ('A', 'F', 'C', date '2026-10-13'),
                                        ('A', 'F', 'C', date '2026-10-13'),
                                        ('B', 'F', null, date '2026-10-13'))
                  as t(cusip, share_class_figi, composite_figi, mapped_on))
            to '{staged}' (format parquet)""")
        with pytest.raises(AuditFailure, match="1 duplicate CUSIPs.*one FIGI and not the other"):
            openfigi.audit(staged)

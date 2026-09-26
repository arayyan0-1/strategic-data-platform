"""Ticker details: monthly dates, the fetch with its failure limit, and the audit. The
vendor is a fake. No test uses the network."""
import datetime as dt

import pytest

from sdp import dal
from sdp.ingest import massive_ticker_details as td
from sdp.ingest.common import AuditFailure

D = dt.date
MONTH_END = D(2024, 1, 31)


def test_month_end_sessions_take_the_last_session_of_each_month():
    ends = td.month_end_sessions(D(2024, 1, 1), D(2024, 4, 15))
    # March 2024 ends on Good Friday, a holiday, so its last session is the 28th.
    assert ends == [D(2024, 1, 31), D(2024, 2, 29), D(2024, 3, 28)]


def test_a_date_that_is_not_a_month_end_is_skipped(tmp_data_root):
    assert td.ingest(D(2024, 1, 30)) is None


@pytest.fixture
def vendor(tmp_data_root, monkeypatch):
    """A fake vendor with N tickers. fail names the tickers that return an error."""

    def install(n=td.MIN_ROWS + 10, fail=(), dupe=False):
        names = [f"T{i:04d}" for i in range(n)]
        monkeypatch.setattr(td, "tickers_for", lambda d: names)

        def get(client, path, params=None, *, attempts=5):
            t = path.rsplit("/", 1)[-1]
            if t in fail:
                raise RuntimeError(f"HTTP 404 on {path}")
            res = {"ticker": "T0000" if dupe and t == "T0001" else t, "type": "CS",
                   "market_cap": 1e9, "share_class_shares_outstanding": 1e7,
                   "sic_code": "3571", "name": f"Company {t}"}
            return {"results": res}

        monkeypatch.setattr(td.rest, "_get", get)
        return names

    return install


def test_a_month_end_publishes_one_row_per_ticker(vendor):
    names = vendor()
    path = td.ingest(MONTH_END)
    assert path == dal.TICKER_DETAILS.partition_file(MONTH_END)
    rel = dal.on_date(dal.TICKER_DETAILS, MONTH_END)
    assert rel.aggregate("count(*), count(distinct ticker), min(sic_code)").fetchone() \
        == (len(names), len(names), "3571")


def test_a_few_unknown_tickers_are_left_out(vendor):
    names = vendor(fail={"T0003", "T0004"})
    td.ingest(MONTH_END)
    (n,) = dal.on_date(dal.TICKER_DETAILS, MONTH_END).aggregate("count(*)").fetchone()
    assert n == len(names) - 2


def test_too_many_failures_stop_the_pull(vendor):
    names = vendor()
    vendor(fail=set(names[: int(len(names) * 0.1)]))
    with pytest.raises(RuntimeError, match="tickers failed"):
        td.ingest(MONTH_END)
    assert not dal.TICKER_DETAILS.partition_file(MONTH_END).exists()


def test_a_duplicate_ticker_is_not_published(vendor):
    vendor(dupe=True)
    with pytest.raises(AuditFailure, match="duplicate"):
        td.ingest(MONTH_END)
    assert not dal.TICKER_DETAILS.partition_file(MONTH_END).exists()

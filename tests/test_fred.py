"""The FRED ingest: parse, fold the pulls, audit, publish, pull at most once a day and
thin the vendor pulls. The series are synthetic and no test uses the network."""
import datetime as dt

import pytest

from sdp import dal
from sdp.ingest import fred
from sdp.ingest.common import AuditFailure

PULL = dt.date(2026, 9, 25)
N = fred.MIN_VALUES + 100
START = dt.date(2020, 1, 1)


def csv(series: str, first: int = 0, n: int = N, value: float = 4.25) -> bytes:
    lines = [f"observation_date,{series}"]
    for i in range(first, first + n):
        d = START + dt.timedelta(days=i)
        lines.append(f"{d},{'' if i % 50 == 7 else value}")
    return ("\n".join(lines) + "\n").encode()


@pytest.fixture
def served(tmp_data_root, monkeypatch):
    """Serve synthetic series in place of FRED. Return the table of served bytes and the
    download log."""
    table = {s: csv(s) for s in fred.SERIES}
    calls: list[str] = []

    def download(series, attempts=4):
        calls.append(series)
        return table[series]

    monkeypatch.setattr(fred, "_download", download)
    return table, calls


def test_a_pull_publishes_every_series_in_long_form(served):
    _, calls = served
    assert fred.ingest(PULL) == dal.FRED.table_file
    assert len(calls) == len(fred.SERIES)
    n_series, n, nulls = dal.fred_series().aggregate(
        "count(distinct series_id), count(*), count(*) - count(value)").fetchone()
    assert n_series == len(fred.SERIES)
    assert n == len(fred.SERIES) * N
    assert nulls > 0, "an empty value is a day with no observation"
    (value,) = dal.fred_series().filter("series_id = 'DGS10' and value is not null") \
        .aggregate("max(value)").fetchone()
    assert value == pytest.approx(4.25), "the table keeps the units of FRED"


def test_a_table_from_today_is_not_pulled_again(served):
    _, calls = served
    fred.ingest(PULL)
    fred.ingest(PULL)
    assert len(calls) == len(fred.SERIES)
    fred.ingest(PULL + dt.timedelta(days=1))
    assert len(calls) == 2 * len(fred.SERIES)


def test_the_fold_keeps_a_date_that_left_the_window_and_the_newest_value_wins(served):
    table, _ = served
    fred.ingest(PULL)
    # The next pull starts 10 days later and revises every value it holds.
    table["BAMLH0A0HYM2"] = csv("BAMLH0A0HYM2", first=10, value=3.5)
    fred.ingest(PULL + dt.timedelta(days=1))
    rows = dict(dal.fred_series().filter("series_id = 'BAMLH0A0HYM2'")
                .select("date, value").fetchall())
    assert rows[START + dt.timedelta(days=5)] == pytest.approx(4.25)
    assert rows[START + dt.timedelta(days=20)] == pytest.approx(3.5)
    assert len(rows) == N + 10


def test_a_new_series_does_not_drop_the_older_pulls_from_the_fold(served, monkeypatch):
    table, _ = served
    fred.ingest(PULL)
    # A series joins the list. The older pull has no file for it, and it still counts.
    monkeypatch.setitem(fred.SERIES, "DGS7", "7-year Treasury")
    table["DGS7"] = csv("DGS7")
    table["BAMLH0A0HYM2"] = csv("BAMLH0A0HYM2", first=10)
    fred.ingest(PULL + dt.timedelta(days=1))
    assert len(fred.vendor_pulls()) == 2
    (n,) = dal.fred_series().filter("series_id = 'BAMLH0A0HYM2'").aggregate("count(*)").fetchone()
    assert n == N + 10, "the dates that only the older pull holds stay"


def test_a_rebuild_folds_the_vendor_pulls_and_does_not_download(served):
    _, calls = served
    fred.ingest(PULL)
    dal.FRED.table_file.unlink()
    fred.ingest(rebuild=True)
    assert len(calls) == len(fred.SERIES)
    assert dal.FRED.table_file.exists()


def test_a_page_that_is_not_a_csv_is_refused(served):
    table, _ = served
    table["VIXCLS"] = b"<html>maintenance</html>"
    with pytest.raises(AuditFailure, match="did not return a CSV"):
        fred.ingest(PULL)
    assert not dal.FRED.table_file.exists()


def test_a_short_series_is_not_published(served):
    table, _ = served
    table["NFCI"] = csv("NFCI", n=10)
    with pytest.raises(AuditFailure, match="NFCI has"):
        fred.ingest(PULL)
    assert not dal.FRED.table_file.exists()


def test_a_rate_in_the_wrong_unit_is_refused_and_a_dollar_amount_is_not(served):
    table, _ = served
    table["WALCL"] = csv("WALCL", value=6_747_704)
    fred.ingest(PULL)
    table["DGS10"] = csv("DGS10", value=4250)
    with pytest.raises(AuditFailure, match="DGS10"):
        fred.ingest(PULL + dt.timedelta(days=1))


def test_thin_keeps_the_first_pull_the_last_month_and_one_pull_a_week(served):
    days = [PULL - dt.timedelta(days=i) for i in range(70, -1, -1)]
    for d in days:
        fred.fetch(d)
    kept_before = {d for d, _ in fred.vendor_pulls()}
    assert kept_before == set(days)
    fred.thin(PULL)
    kept = {d for d, _ in fred.vendor_pulls()}
    assert days[0] in kept and PULL in kept
    assert {d for d in days if (PULL - d).days <= fred.KEEP_DAYS} <= kept
    old = [d for d in kept if (PULL - d).days > fred.KEEP_DAYS and d != days[0]]
    weeks = [d.isocalendar()[:2] for d in old]
    assert len(weeks) == len(set(weeks)), "one pull per ISO week"

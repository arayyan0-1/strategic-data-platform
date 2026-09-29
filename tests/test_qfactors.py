"""The q-factor ingest: find the newest file on the page, parse, audit, publish, and pull
at most once a month. The files are synthetic and no test uses the network."""
import datetime as dt

import pytest

from sdp import dal
from sdp.config import settings
from sdp.ingest import qfactors
from sdp.ingest.common import AuditFailure

PULL = dt.date(2026, 9, 25)
N = qfactors.MIN_ROWS + 50

PAGE = b'''<a href="/uploads/x/q5_factors_daily_2024.csv">2024</a>
<a href="/uploads/x/q5_factors_daily_2025.csv">2025</a>
<a href="/uploads/x/q5_factors_monthly_2025.csv">monthly</a>'''


def factor_file(n: int = N, *, bad: str | None = None) -> bytes:
    start = dt.date(1967, 1, 3)
    rows = [f"{start + dt.timedelta(days=i)},0.0187,0.10,-0.20,0.05,0.01,-0.03"
            for i in range(n)]
    if bad == "duplicate":
        rows.append(rows[-1])
    return ("date,R_F,R_MKT,R_ME,R_IA,R_ROE,R_EG\n" + "\n".join(rows) + "\n").encode()


@pytest.fixture
def site(tmp_data_root, monkeypatch):
    served = {settings.qfactors_page_url: PAGE,
              "https://global-q.org/uploads/x/q5_factors_daily_2025.csv": factor_file()}
    calls: list[str] = []

    def download(url, attempts=4):
        calls.append(url)
        return served[url]

    monkeypatch.setattr(qfactors, "_download", download)
    return served, calls


def test_a_pull_takes_the_newest_vintage_as_fractions(site):
    _, calls = site
    assert qfactors.ingest(PULL) == dal.QFACTORS.table_file
    assert calls[-1].endswith("q5_factors_daily_2025.csv")
    r_f, r_me, vintage = dal.q_factors().limit(1).select("r_f, r_me, vintage").fetchone()
    assert r_f == pytest.approx(0.000187) and r_me == pytest.approx(-0.002)
    assert vintage == "2025"


def test_a_fresh_table_is_not_pulled_again(site):
    _, calls = site
    qfactors.ingest(PULL)
    qfactors.ingest(PULL + dt.timedelta(days=qfactors.MAX_AGE_DAYS - 1))
    assert len(calls) == 2
    qfactors.ingest(PULL + dt.timedelta(days=qfactors.MAX_AGE_DAYS))
    assert len(calls) == 4


def test_a_rebuild_reads_the_vendor_pull_and_does_not_download(site):
    _, calls = site
    qfactors.ingest(PULL)
    dal.QFACTORS.table_file.unlink()
    qfactors.ingest(rebuild=True)
    assert len(calls) == 2
    assert dal.QFACTORS.table_file.exists()


def test_a_page_with_no_daily_file_is_refused(site):
    served, _ = site
    served[settings.qfactors_page_url] = b"<html>no links</html>"
    with pytest.raises(AuditFailure, match="links no daily"):
        qfactors.ingest(PULL)


def test_a_duplicate_date_is_not_published(site):
    served, _ = site
    served["https://global-q.org/uploads/x/q5_factors_daily_2025.csv"] = \
        factor_file(bad="duplicate")
    with pytest.raises(AuditFailure, match="duplicate"):
        qfactors.ingest(PULL)
    assert not dal.QFACTORS.table_file.exists()

"""The Fama-French ingest: parse, audit, publish, and pull at most once a week. The
library files are synthetic and no test uses the network."""
import datetime as dt
import io
import zipfile

import pytest

from sdp import dal
from sdp.ingest import french
from sdp.ingest.common import AuditFailure

PULL = dt.date(2026, 9, 25)
N = french.MIN_ROWS + 50


def _zip(header: str, rows: list[str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("data.CSV", header + "\n\n" + "\n".join(rows) + "\n\n Annual Factors\n")
    return buf.getvalue()


def _dates(n: int) -> list[str]:
    start = dt.date(1990, 1, 1)
    return [(start + dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(n)]


def files(n: int = N, *, bad: str | None = None) -> dict[str, bytes]:
    dates = _dates(n)
    five = [f"{d},0.10,-0.05,0.02,0.01,-0.01,0.01" for d in dates]
    if bad == "duplicate":
        five.append(five[-1])
    if bad == "huge":
        five[5] = f"{dates[5]},60.0,0,0,0,0,0.01"
    mom = [f"{d},0.20" for d in dates[1:]]  # Momentum starts one day later.
    head = "This file was created by CMPT_ME_BEME_OP_INV_RETS_DAILY using the 202607 CRSP database."
    rev = [f"{d},0.30" for d in dates]
    vw = [f"{d}," + ",".join(["0.40"] * 12) for d in dates]
    vw[0] = f"{dates[0]},-99.99," + ",".join(["0.40"] * 11)
    ew = [f"{d}," + ",".join(["9.00"] * 12) for d in dates]
    industries = ("  Average Value Weighted Returns -- Daily\n,NoDur,Durbl,Manuf\n"
                  + "\n".join(vw) + "\n\n  Average Equal Weighted Returns -- Daily\n"
                  + ",NoDur,Durbl,Manuf\n" + "\n".join(ew))
    return {
        "F-F_Research_Data_5_Factors_2x3_daily_CSV.zip": _zip(head, five),
        "F-F_Momentum_Factor_daily_CSV.zip": _zip(head, mom),
        "F-F_ST_Reversal_Factor_daily_CSV.zip": _zip(head, rev),
        "F-F_LT_Reversal_Factor_daily_CSV.zip": _zip(head, rev),
        "12_Industry_Portfolios_daily_CSV.zip": _zip(head, [industries]),
    }


@pytest.fixture
def library(tmp_data_root, monkeypatch):
    """Serve synthetic files in place of the library. Return the download log."""
    served: dict[str, bytes] = files()
    calls: list[str] = []

    def download(name, attempts=4):
        calls.append(name)
        return served[name]

    monkeypatch.setattr(french, "_download", download)
    return served, calls


def test_a_pull_publishes_the_factors_as_fractions(library):
    _, calls = library
    path = french.ingest(PULL)
    assert path == dal.FRENCH.table_file
    assert len(calls) == len(french.FILES)
    row = dal.french_factors().order("date").limit(2).fetchall()
    first, second = row
    assert first[1] == pytest.approx(0.001)          # 0.10 percent
    assert first[7] is None and second[7] == pytest.approx(0.002)
    assert dal.french_factors().aggregate("max(crsp_month), max(vendor_pull_date)").fetchone() \
        == ("202607", PULL)


def test_a_fresh_table_is_not_pulled_again(library):
    _, calls = library
    french.ingest(PULL)
    french.ingest(PULL + dt.timedelta(days=french.MAX_AGE_DAYS - 1))
    assert len(calls) == len(french.FILES)
    french.ingest(PULL + dt.timedelta(days=french.MAX_AGE_DAYS))
    assert len(calls) == 2 * len(french.FILES)


def test_a_rebuild_reads_the_vendor_pull_and_does_not_download(library):
    _, calls = library
    french.ingest(PULL)
    dal.FRENCH.table_file.unlink()
    french.ingest(rebuild=True)
    assert len(calls) == len(french.FILES)
    assert dal.FRENCH.table_file.exists()


@pytest.mark.parametrize("bad", ["duplicate", "huge"])
def test_a_bad_file_is_not_published(library, bad):
    served, _ = library
    served.update(files(bad=bad))
    with pytest.raises(AuditFailure):
        french.ingest(PULL)
    assert not dal.FRENCH.table_file.exists()


def test_a_pull_that_loses_history_keeps_the_old_table(library):
    served, _ = library
    french.ingest(PULL)
    before = dal.FRENCH.table_file.read_bytes()
    served.update(files(n=N - 10))
    with pytest.raises(AuditFailure, match="does not remove history"):
        french.ingest(PULL, force=True)
    assert dal.FRENCH.table_file.read_bytes() == before


def test_the_industries_take_the_value_weighted_section_and_a_missing_value_is_null(library):
    french.ingest(PULL)
    rows = dal.french_factors().order("date").limit(2) \
        .select("ind_nodur, ind_durbl, st_rev, lt_rev").fetchall()
    assert rows[0][0] is None, "-99.99 marks a missing value"
    assert rows[0][1] == pytest.approx(0.004), "the equal-weighted section is not read"
    assert rows[1][0] == pytest.approx(0.004)
    assert rows[0][2] == pytest.approx(0.003) and rows[0][3] == pytest.approx(0.003)

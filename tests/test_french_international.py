"""Tests for the French international ingest against synthetic library files."""
import datetime as dt
import io
import logging
import zipfile

import pytest

from sdp import dal
from sdp.ingest import french_international as fi
from sdp.ingest.common import AuditFailure

PULL = dt.date(2026, 9, 25)
N = 30   # Months from 2020-01 to 2022-06.


def _months(n: int = N) -> list[str]:
    return [f"{2020 + i // 12}{i % 12 + 1:02d}" for i in range(n)]


def _zip(*members: tuple[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in members:
            z.writestr(name, text)
    return buf.getvalue()


def _csv(cols: list[str], rows: list[list[str]], *, head: str = "") -> str:
    """A library CSV: preamble, the monthly section, then an annual section."""
    lines = ["This file was created using the 202608 CRSP database.", "", head, "",
             "," + ",".join(cols)]
    lines += [f"{r[0]}    ," + ",".join(f"{v:>8}" for v in r[1:]) for r in rows]
    lines += ["", "", " Annual Factors: January-December", "," + ",".join(cols),
              "2020   ," + ",".join("999.99" for _ in cols)]
    return "\n".join(lines) + "\n"


def _factor_file(cols: list[str], n: int, base: float) -> bytes:
    rows = [[m, *[f"{base + 0.01 * i + j:.2f}" for j in range(len(cols))]]
            for i, m in enumerate(_months(n))]
    return _zip(("f.csv", _csv(cols, rows)))


def _portfolio_file(cols: list[str], n: int, base: float) -> bytes:
    """Value-weighted rows, then equal-weighted rows that the parser must not read."""
    vw = [[m, *[f"{base + 0.01 * i + j:.4f}" for j in range(len(cols))]]
          for i, m in enumerate(_months(n))]
    ew = [[m, *["9.0000"] * len(cols)] for m in _months(n)]
    head = ",".join([""] + cols)
    text = ("Portfolios.\n\n  Average Value Weighted Returns -- Monthly\n" + head + "\n"
            + "\n".join(f"{r[0]},  " + ",  ".join(r[1:]) for r in vw)
            + "\n\n  Average Equal Weighted Returns -- Monthly\n" + head + "\n"
            + "\n".join(f"{r[0]},  " + ",  ".join(r[1:]) for r in ew)
            + "\n\n  Average Value Weighted Returns -- Annual\n" + head + "\n2020,  999.9\n")
    return _zip(("p.csv", text))


def _country(title: str, n: int, base: float, *, missing_first: bool = False) -> str:
    rows = []
    for i, m in enumerate(_months(n)):
        mkt = "-99.99" if missing_first and i == 0 else f"{base + 0.01 * i:.2f}"
        rows.append(f"{m} {mkt:>6}" + " 1.00" * 8 + " -99.99")
    return ("\n".join([
        "", title, "          -- BE/ME --   --- E/P ---   --- CE/P --   ------  Yld  -----",
        "          Mkt   High    Low   High    Low   High    Low   High    Low   Zero", *rows,
        "", "     Value-Weight Local  Returns      All 4 Data Items Not Reqd",
        "          Mkt   High    Low   High    Low   High    Low   High    Low   Zero",
        f"{_months(n)[0]}   9.00" + " 1.00" * 9]) + "\n")


TITLE = "     Value-Weight Dollar Returns      All 4 Data Items Not Reqd"


def countries(n: int = N, *, title: str = TITLE, extra: bool = False) -> bytes:
    members = [("UK.Dat", _country(title, n, 2.0, missing_first=True)),
               ("HongKong.Dat", _country(title, n, 3.0)),
               ("Austrlia.Dat", _country(title, n - 6, 1.0))]
    if extra:
        members.append(("Zzzzzzz.Dat", _country(title, n, 1.0)))
    return _zip(*members)


def files(n: int = N, *, bad: str | None = None) -> dict[str, bytes]:
    out: dict[str, bytes] = {}
    for name, series in fi.FILES.items():
        if series is None:
            out[name] = countries(n)
        elif "5_Factors" in name:
            out[name] = _factor_file(["Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"], n, 1.0)
        elif "Mom_Factor" in name or "MOM_Factor" in name:
            out[name] = _factor_file(["WML"], n, 0.5)
        elif name.startswith("F-F_Research"):
            out[name] = _factor_file(["Mkt-RF", "SMB", "HML", "RF"], n, 1.0)
        else:
            cols = [f"C{j}" for j in range(25 if name.startswith("25") else 6)]
            out[name] = _portfolio_file(cols, n, 0.5)
    name = "Developed_5_Factors_CSV.zip"
    cols = ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"]
    rows = [[m, *["1.00"] * 6] for m in _months(n)]
    if bad == "duplicate":
        out[name] = _zip(("f.csv", _csv(cols, rows + [rows[-1]])))
    elif bad == "huge":
        rows[5][1] = "250.00"
        out[name] = _zip(("f.csv", _csv(cols, rows)))
    elif bad == "empty":
        out[name] = _zip(("f.csv", _csv(cols, [])))
    elif bad == "short_row":
        rows[3] = rows[3][:-1]
        out[name] = _zip(("f.csv", _csv(cols, rows)))
    elif bad == "bad_month":
        rows[2][0] = "202013"
        out[name] = _zip(("f.csv", _csv(cols, rows)))
    return out


@pytest.fixture
def library(tmp_data_root, monkeypatch):
    """Serve synthetic files in place of the library. Return the files and the download log."""
    served: dict[str, bytes] = files()
    calls: list[str] = []

    def download(name, attempts=4):
        calls.append(name)
        return served[name]

    monkeypatch.setattr(fi, "_download", download)
    return served, calls


def _value(file: str, series: str, column: str, month: dt.date):
    return dal.french_international().filter(
        f"file = '{file}' and series = '{series}' and column_name = '{column}' "
        f"and month = date '{month}'").fetchall()


def test_a_pull_publishes_the_returns_as_fractions_on_the_last_day_of_the_month(library):
    _, calls = library
    path = fi.ingest(PULL)
    assert path == dal.FRENCH_INTERNATIONAL.table_file
    assert len(calls) == len(fi.FILES)
    ((v, pull),) = [(r[4], r[5]) for r in _value(
        "Developed_5_Factors_CSV.zip", "monthly", "Mkt-RF", dt.date(2020, 2, 29))]
    assert v == pytest.approx(0.0101), "1.01 percent in the second month of 2020, a leap year"
    assert pull == PULL


def test_only_the_first_monthly_section_is_read(library):
    fi.ingest(PULL)
    rel = dal.french_international()
    assert rel.filter("abs(value) > 1").count("*").fetchone() == (0,), "an annual row"
    n, top = rel.filter("file = '6_Portfolios_2x3_CSV.zip'").aggregate(
        "count(distinct month), max(value)").fetchone()
    assert n == N
    assert top < 0.08, "the equal-weighted section holds 9 percent"


def test_the_country_file_gives_the_market_of_each_country_by_name(library):
    fi.ingest(PULL)
    rows = dal.french_international().filter("file = 'F-F_International_Countries.zip'") \
        .aggregate("series, count(*), min(month), max(column_name)").order("series").fetchall()
    assert [r[0] for r in rows] == ["Australia", "Hong Kong", "United Kingdom"]
    assert [r[1] for r in rows] == [N - 6, N, N - 1], "-99.99 is a missing month, not a row"
    assert {r[3] for r in rows} == {"Mkt"}
    assert _value(fi.COUNTRIES, "Hong Kong", "Mkt", dt.date(2020, 1, 31))[0][4] \
        == pytest.approx(0.03)
    assert dal.french_international().filter("series = 'United Kingdom'").aggregate(
        "min(month)").fetchone() == (dt.date(2020, 2, 29),)


def test_an_unknown_country_member_is_skipped_with_a_warning(library, caplog):
    served, _ = library
    served[fi.COUNTRIES] = countries(extra=True)
    with caplog.at_level(logging.WARNING, logger=fi.log.name):
        fi.ingest(PULL)
    assert "Zzzzzzz.Dat" in caplog.text
    assert dal.french_international().filter("series like 'Zz%'").count("*").fetchone() == (0,)


def test_a_fresh_table_is_not_pulled_again(library):
    _, calls = library
    fi.ingest(PULL)
    fi.ingest(PULL + dt.timedelta(days=fi.MAX_AGE_DAYS - 1))
    assert len(calls) == len(fi.FILES)
    fi.ingest(PULL + dt.timedelta(days=fi.MAX_AGE_DAYS))
    assert len(calls) == 2 * len(fi.FILES)


def test_a_rebuild_reads_the_vendor_pull_and_does_not_download(library):
    _, calls = library
    fi.ingest(PULL)
    dal.FRENCH_INTERNATIONAL.table_file.unlink()
    fi.ingest(rebuild=True)
    assert len(calls) == len(fi.FILES)
    assert dal.FRENCH_INTERNATIONAL.table_file.exists()


@pytest.mark.parametrize("bad", ["duplicate", "huge", "empty", "short_row", "bad_month"])
def test_a_bad_file_is_not_published(library, bad):
    served, _ = library
    served.update(files(bad=bad))
    with pytest.raises(AuditFailure):
        fi.ingest(PULL)
    assert not dal.FRENCH_INTERNATIONAL.table_file.exists()


def test_a_country_layout_that_is_not_dollar_returns_is_refused(library):
    served, _ = library
    served[fi.COUNTRIES] = countries(title="     Value-Weight Local  Returns      All 4 Data "
                                           "Items Not Reqd")
    with pytest.raises(AuditFailure, match="dollar returns"):
        fi.ingest(PULL)
    assert not dal.FRENCH_INTERNATIONAL.table_file.exists()


def test_a_pull_that_loses_history_keeps_the_old_table(library):
    served, _ = library
    fi.ingest(PULL)
    before = dal.FRENCH_INTERNATIONAL.table_file.read_bytes()
    served.update(files(n=N - 3))
    with pytest.raises(AuditFailure, match="does not remove history"):
        fi.ingest(PULL, force=True)
    assert dal.FRENCH_INTERNATIONAL.table_file.read_bytes() == before


def test_a_pull_that_drops_a_country_keeps_the_old_table(library):
    served, _ = library
    fi.ingest(PULL)
    served[fi.COUNTRIES] = _zip(("UK.Dat", _country(TITLE, N, 2.0)),
                                ("HongKong.Dat", _country(TITLE, N, 3.0)))
    with pytest.raises(AuditFailure, match="does not remove history"):
        fi.ingest(PULL, force=True)


def test_a_restated_value_and_a_new_month_are_published(library):
    served, _ = library
    fi.ingest(PULL)
    served.update(files(n=N + 2))
    served["Japan_5_Factors_CSV.zip"] = _factor_file(
        ["Mkt-RF", "SMB", "HML", "RMW", "CMA", "RF"], N + 2, 2.0)
    fi.ingest(PULL, force=True)
    rel = dal.french_international().filter("file = 'Japan_5_Factors_CSV.zip'")
    assert rel.aggregate("max(month)").fetchone() == (dt.date(2022, 8, 31),)
    assert _value("Japan_5_Factors_CSV.zip", "monthly", "Mkt-RF",
                  dt.date(2020, 1, 31))[0][4] == pytest.approx(0.02)


def test_a_file_that_ends_long_before_the_pull_warns(library, caplog):
    with caplog.at_level(logging.WARNING, logger=fi.log.name):
        fi.ingest(dt.date(2022, 10, 25))
    assert "ends in 2022-06-30" not in caplog.text, "four months is not stale"
    with caplog.at_level(logging.WARNING, logger=fi.log.name):
        fi.ingest(dt.date(2022, 11, 25), force=True)
    assert "ends in 2022-06-30" in caplog.text

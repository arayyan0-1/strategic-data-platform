"""ingest() skips an unchanged republish. thin_vendor_pulls() keeps one dividend
pull per ISO week after 30 days. No test touches the network.
"""
import datetime as dt
import gzip
import json
import os

import duckdb
import pytest

from sdp import dal
from sdp.config import settings
from sdp.ingest import massive_corporate_actions as ca

D = dt.date
SPLITS, DIVIDENDS = "massive_splits", "massive_dividends"
P1, P2 = D(2026, 8, 9), D(2026, 8, 10)

ROWS = {
    SPLITS: {"id": "a", "ticker": "AAA", "execution_date": "2020-01-02",
             "split_from": 1.0, "split_to": 2.0, "adjustment_type": "forward_split",
             "historical_adjustment_factor": 0.5},
    DIVIDENDS: {"id": "d", "ticker": "AAA", "ex_dividend_date": "2020-01-02",
                "cash_amount": 0.5, "currency": "USD", "frequency": 4,
                "historical_adjustment_factor": 0.99},
}

# 120 daily pulls from a Wednesday to a Wednesday. The last 30 days start on
# Monday 2026-08-24, so the older pulls end on Sunday 2026-08-23.
FIRST, NEWEST, CUTOFF = D(2026, 5, 27), D(2026, 9, 23), D(2026, 8, 24)
DATES = [FIRST + dt.timedelta(days=i) for i in range(120)]
SUNDAYS = [d for d in DATES if d.isoweekday() == 7 and d < CUTOFF]
RECENT = [d for d in DATES if d >= CUTOFF]


def test_the_calendar_of_the_thinning_tests():
    assert DATES[-1] == NEWEST
    assert (NEWEST - CUTOFF).days == 30
    assert len(SUNDAYS) == 13 and SUNDAYS[0] == D(2026, 5, 31)
    assert len(RECENT) == 31


def _vendor(dataset, pull, *, gz=True):
    """Write a valid one-row vendor pull."""
    suffix = ".ndjson.gz" if gz else ".ndjson"
    path = settings.vendor_dir / dataset / f"{pull:%Y-%m-%d}{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(ROWS[dataset]) + "\n"
    if gz:
        with gzip.open(path, "wt", encoding="utf-8") as fh:
            fh.write(line)
    else:
        path.write_text(line, encoding="utf-8")
    return path


def _touch(dataset, dates):
    """Write empty vendor files. The thinning reads only the file names."""
    vdir = settings.vendor_dir / dataset
    vdir.mkdir(parents=True, exist_ok=True)
    for d in dates:
        (vdir / f"{d:%Y-%m-%d}.ndjson.gz").touch()


def _kept(dataset):
    return [d for d, _ in ca.vendor_pulls(dataset)]


def _age(dataset, pull):
    """Set old mtimes, vendor file before table, so that a republish shows."""
    for f in ca._vendor_files(dataset, pull):
        os.utime(f, (900_000_000, 900_000_000))
    raw = ca.raw_path(dataset)
    os.utime(raw, (1_000_000_000, 1_000_000_000))
    return raw.stat().st_mtime_ns


def _published(ds):
    return dal.current(ds).project("vendor_pull_date").fetchall()


@pytest.fixture
def fetch(tmp_data_root, monkeypatch):
    """Replace the network fetch with a local one that acts as dump_ndjson does.
    Return the list of calls."""
    calls = []

    def fake(dataset, path, params, pull_date, *, force=False, **_):
        calls.append((dataset, pull_date, force))
        existing = ca._vendor_files(dataset, pull_date)
        if existing and not force:
            return existing[0]
        return _vendor(dataset, pull_date)

    monkeypatch.setattr(ca, "dump_ndjson", fake)
    return calls


@pytest.fixture
def builds(monkeypatch):
    """Record the pull date of each call to build()."""
    calls = []
    real = ca.build

    def counting(dataset, vendor_file, pull_date):
        calls.append(pull_date)
        return real(dataset, vendor_file, pull_date)

    monkeypatch.setattr(ca, "build", counting)
    return calls


class TestSkipAnUnchangedRepublish:
    def test_a_second_ingest_of_the_same_pull_changes_nothing(self, fetch, builds):
        first = ca.ingest(SPLITS, P1)
        mtime = _age(SPLITS, P1)

        assert ca.ingest(SPLITS, P1) == first
        assert first.stat().st_mtime_ns == mtime
        assert builds == [P1]
        assert fetch == [(SPLITS, P1, False)]

    def test_a_plain_ndjson_vendor_file_also_counts(self, fetch, builds):
        _vendor(SPLITS, P1, gz=False)
        ca.rebuild(SPLITS)
        mtime = _age(SPLITS, P1)

        ca.ingest(SPLITS, P1)
        assert ca.raw_path(SPLITS).stat().st_mtime_ns == mtime
        assert builds == [P1]
        assert fetch == []

    def test_a_new_pull_date_publishes(self, fetch, builds):
        ca.ingest(SPLITS, P1)
        ca.ingest(SPLITS, P2)
        assert builds == [P1, P2]
        assert _published(dal.SPLITS) == [(P2,)]

    def test_force_fetches_and_publishes_again(self, fetch, builds):
        first = ca.ingest(SPLITS, P1)
        mtime = _age(SPLITS, P1)

        ca.ingest(SPLITS, P1, force=True)
        assert builds == [P1, P1]
        assert fetch[-1] == (SPLITS, P1, True)
        assert first.stat().st_mtime_ns != mtime

    def test_a_vendor_file_that_did_not_build_the_table_publishes(self, fetch, builds):
        _vendor(SPLITS, P1)
        _vendor(SPLITS, P2)
        ca.rebuild(SPLITS, P1)

        ca.ingest(SPLITS, P2)
        assert builds == [P1, P2]
        assert _published(dal.SPLITS) == [(P2,)]

    def test_a_vendor_file_with_no_published_table_publishes(self, fetch, builds):
        """An earlier run can fetch the file and then fail its audit."""
        _vendor(SPLITS, P1)
        ca.ingest(SPLITS, P1)
        assert builds == [P1]
        assert _published(dal.SPLITS) == [(P1,)]

    def test_a_vendor_file_newer_than_the_table_publishes(self, fetch, builds):
        """A forced fetch can write new bytes and then fail its audit. The next
        run builds those bytes again, so the failure stays visible."""
        ca.ingest(SPLITS, P1)
        _age(SPLITS, P1)
        os.utime(ca._vendor_files(SPLITS, P1)[0])

        ca.ingest(SPLITS, P1)
        assert builds == [P1, P1]

    def test_a_table_with_no_pull_date_gets_a_new_publish(self, fetch, builds):
        raw = ca.raw_path(SPLITS)
        raw.parent.mkdir(parents=True)
        duckdb.execute(
            f"copy (select 'a' as id, 'AAA' as ticker, 0.5 as historical_adjustment_factor)"
            f" to '{raw}' (format parquet)")
        _vendor(SPLITS, P1)
        _age(SPLITS, P1)

        ca.ingest(SPLITS, P1)
        assert builds == [P1]
        assert _published(dal.SPLITS) == [(P1,)]


class TestThinVendorPulls:
    def test_it_keeps_the_first_pull_the_last_30_days_and_one_per_older_week(
        self, tmp_data_root
    ):
        _touch(DIVIDENDS, DATES)
        deleted = ca.thin_vendor_pulls(DIVIDENDS)

        assert _kept(DIVIDENDS) == sorted({FIRST, *SUNDAYS, *RECENT})
        assert len(deleted) == 120 - 45
        assert not any(p.exists() for p in deleted)

    def test_the_kept_pull_of_an_older_week_is_the_newest_of_that_week(
        self, tmp_data_root
    ):
        """Without a Sunday pull, the Saturday pull represents the week."""
        _touch(DIVIDENDS, [d for d in DATES if d != D(2026, 7, 12)])
        ca.thin_vendor_pulls(DIVIDENDS)

        kept = _kept(DIVIDENDS)
        assert D(2026, 7, 11) in kept
        assert D(2026, 7, 10) not in kept

    def test_it_keeps_the_pull_of_the_published_table(self, ca_lake):
        _touch(DIVIDENDS, DATES)
        ca_lake(dal.DIVIDENDS, D(2026, 6, 10), [("d", "AAA", D(2020, 1, 2), 0.99)])

        ca.thin_vendor_pulls(DIVIDENDS)
        assert _kept(DIVIDENDS) == sorted({FIRST, D(2026, 6, 10), *SUNDAYS, *RECENT})

    def test_dry_run_deletes_nothing_and_returns_the_plan(self, tmp_data_root):
        _touch(DIVIDENDS, DATES)
        plan = ca.thin_vendor_pulls(DIVIDENDS, dry_run=True)

        assert len(plan) == 120 - 45
        assert _kept(DIVIDENDS) == DATES
        assert ca.thin_vendor_pulls(DIVIDENDS) == plan

    def test_a_date_with_two_file_names_loses_both(self, tmp_data_root):
        _touch(DIVIDENDS, DATES)
        plain = settings.vendor_dir / DIVIDENDS / "2026-06-02.ndjson"
        plain.touch()

        ca.thin_vendor_pulls(DIVIDENDS)
        assert not plain.exists()
        assert D(2026, 6, 2) not in _kept(DIVIDENDS)

    def test_a_future_vendor_file_does_not_move_the_window(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(ca, "_today", lambda: NEWEST)
        _touch(DIVIDENDS, [*DATES, D(2026, 12, 1)])

        ca.thin_vendor_pulls(DIVIDENDS)
        assert _kept(DIVIDENDS) == sorted({FIRST, *SUNDAYS, *RECENT, D(2026, 12, 1)})

    def test_it_returns_nothing_without_vendor_pulls(self, tmp_data_root):
        assert ca.thin_vendor_pulls(DIVIDENDS) == []


class TestIngestThinsDividends:
    def test_an_ingest_of_dividends_thins_the_archive(self, fetch):
        _touch(DIVIDENDS, DATES[:-1])
        ca.ingest(DIVIDENDS, NEWEST)
        assert _kept(DIVIDENDS) == sorted({FIRST, *SUNDAYS, *RECENT})

    def test_an_ingest_of_splits_does_not_thin(self, fetch):
        _touch(SPLITS, DATES[:-1])
        ca.ingest(SPLITS, NEWEST)
        assert _kept(SPLITS) == DATES

    def test_a_rebuild_does_not_thin(self, tmp_data_root):
        _touch(DIVIDENDS, DATES[:-1])
        _vendor(DIVIDENDS, NEWEST)
        ca.rebuild(DIVIDENDS)
        assert _kept(DIVIDENDS) == DATES

    def test_a_failed_thinning_does_not_fail_the_ingest(self, fetch, monkeypatch, caplog):
        def broken(dataset, **_):
            raise OSError("disk error")

        monkeypatch.setattr(ca, "thin_vendor_pulls", broken)
        assert ca.ingest(DIVIDENDS, P1) == ca.raw_path(DIVIDENDS)
        assert _published(dal.DIVIDENDS) == [(P1,)]
        assert "thin_vendor_pulls failed" in caplog.text


class TestCommandLine:
    def test_thin_dry_run_prints_the_plan_and_deletes_nothing(self, tmp_data_root, capsys):
        _touch(DIVIDENDS, DATES)
        ca.main([DIVIDENDS, "--thin", "--dry-run"])

        out = capsys.readouterr().out
        assert "75 files to delete" in out
        assert "2026-05-28.ndjson.gz" in out
        assert _kept(DIVIDENDS) == DATES

    def test_thin_deletes_and_prints_the_files(self, tmp_data_root, capsys):
        _touch(DIVIDENDS, DATES)
        ca.main(["--thin"])

        out = capsys.readouterr().out
        assert "75 files deleted" in out
        assert _kept(DIVIDENDS) == sorted({FIRST, *SUNDAYS, *RECENT})

    def test_rebuild_still_works_with_names_on_both_sides(self, tmp_data_root):
        _vendor(SPLITS, P1)
        _vendor(DIVIDENDS, P2)
        ca.main([SPLITS, "--rebuild", DIVIDENDS])
        assert _published(dal.SPLITS) == [(P1,)]
        assert _published(dal.DIVIDENDS) == [(P2,)]

    @pytest.mark.parametrize("argv", [
        ["--dry-run"],
        ["massive_dividend", "--thin"],
        ["--thin", "--rebuild"],
        ["--force", "--rebuild"],
        [SPLITS, "--thin"],
    ])
    def test_it_refuses_a_bad_command(self, tmp_data_root, argv):
        with pytest.raises(SystemExit):
            ca.main(argv)

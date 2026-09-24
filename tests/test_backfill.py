"""The date-loop runner.

Each test replaces one entry in backfill.TARGETS with a plain function. No HTTP
or mocks needed. One bad date does not stop the run.
"""
import datetime as dt
import json
from pathlib import Path

import pytest

from sdp import backfill

D = dt.date

# 2024-01-01 is a holiday. The XNYS sessions in this range are 01-02, 01-03,
# 01-04, 01-05 and 01-08. 01-06 and 01-07 are a weekend.
START, END = D(2024, 1, 1), D(2024, 1, 8)
SESSIONS = [D(2024, 1, 2), D(2024, 1, 3), D(2024, 1, 4), D(2024, 1, 5), D(2024, 1, 8)]


@pytest.fixture
def fake_ingest(monkeypatch):
    """Replace the day_aggs target with a function that a test controls."""

    def install(fail_on: tuple = (), skip_on: tuple = ()):
        calls: list[dt.date] = []

        def ingest(d: dt.date, *, refetch: bool = False, rebuild: bool = False):
            calls.append(d)
            if d in fail_on:
                raise RuntimeError(f"vendor error on {d}")
            if d in skip_on:
                return None
            return Path(f"/fake/{d}.parquet")

        monkeypatch.setitem(backfill.TARGETS, "day_aggs", ingest)
        return calls

    return install


class TestSessions:
    def test_returns_xnys_sessions_only(self):
        assert backfill.sessions(START, END) == SESSIONS

    def test_raises_when_start_is_after_end(self):
        with pytest.raises(SystemExit, match="after the end date"):
            backfill.sessions(D(2024, 2, 1), D(2024, 1, 1))


class TestFailureIsolation:
    """One bad date does not stop the run."""

    def test_a_failing_date_does_not_stop_the_run(self, tmp_data_root, fake_ingest):
        calls = fake_ingest(fail_on=(D(2024, 1, 4),))
        failures = backfill.backfill("day_aggs", START, END)

        assert calls == SESSIONS, "every date must be attempted"
        assert [d for d, _ in failures] == [D(2024, 1, 4)]

    def test_every_date_can_fail_without_an_exception_escaping(
        self, tmp_data_root, fake_ingest
    ):
        fake_ingest(fail_on=tuple(SESSIONS))
        failures = backfill.backfill("day_aggs", START, END)
        assert len(failures) == len(SESSIONS)

    def test_the_failure_carries_the_type_and_the_message(
        self, tmp_data_root, fake_ingest
    ):
        fake_ingest(fail_on=(D(2024, 1, 3),))
        (_, message), = backfill.backfill("day_aggs", START, END)
        assert message.startswith("RuntimeError:")
        assert "vendor error on 2024-01-03" in message

    def test_a_clean_run_reports_no_failure(self, tmp_data_root, fake_ingest):
        fake_ingest()
        assert backfill.backfill("day_aggs", START, END) == []


class TestTargetGuards:
    """A current-state dataset must never be reachable through this runner."""

    @pytest.mark.parametrize(
        "target,dataset",
        [("splits", "massive_splits"), ("dividends", "massive_dividends"),
         ("massive_splits", "massive_splits"), ("massive_dividends", "massive_dividends")],
    )
    def test_a_current_state_target_is_refused(self, target, dataset):
        with pytest.raises(SystemExit) as exc:
            backfill.backfill(target, START, END)
        message = str(exc.value)
        assert "cannot backfill" in message
        assert dataset in message
        assert "massive_corporate_actions" in message, "the message must give the fix"

    def test_an_unknown_target_is_refused_and_lists_the_valid_ones(self):
        with pytest.raises(SystemExit) as exc:
            backfill.backfill("nonsense", START, END)
        message = str(exc.value)
        assert "nonsense" in message
        assert "day_aggs" in message
        assert "tickers" in message


class TestFlags:
    def test_limit_truncates_the_session_list(self, tmp_data_root, fake_ingest):
        calls = fake_ingest()
        backfill.backfill("day_aggs", START, END, limit=2)
        assert calls == SESSIONS[:2]

    def test_dry_run_calls_no_ingest_function(self, tmp_data_root, fake_ingest):
        calls = fake_ingest()
        assert backfill.backfill("day_aggs", START, END, dry_run=True) == []
        assert calls == []

    def test_a_limit_of_zero_ingests_nothing(self, tmp_data_root, fake_ingest):
        calls = fake_ingest()
        assert backfill.backfill("day_aggs", START, END, limit=0) == []
        assert calls == []

    @pytest.mark.parametrize("dry_run", [False, True])
    def test_a_range_with_no_session_is_not_an_error(
        self, tmp_data_root, fake_ingest, dry_run
    ):
        """2024-01-01 is a holiday."""
        calls = fake_ingest()
        assert backfill.backfill("day_aggs", START, START, dry_run=dry_run) == []
        assert calls == []


class TestModes:
    """refetch downloads again. rebuild reads vendor/ only. They exclude each other."""

    @pytest.fixture
    def seen(self, monkeypatch):
        seen: list[tuple[bool, bool]] = []

        def ingest(d, *, refetch=False, rebuild=False):
            seen.append((refetch, rebuild))
            return Path("/fake")

        monkeypatch.setitem(backfill.TARGETS, "day_aggs", ingest)
        return seen

    def test_the_default_sets_neither_mode(self, tmp_data_root, seen):
        backfill.backfill("day_aggs", START, END, limit=1)
        assert seen == [(False, False)]

    def test_refetch_reaches_the_ingest_function(self, tmp_data_root, seen):
        backfill.backfill("day_aggs", START, END, refetch=True, limit=1)
        assert seen == [(True, False)]

    def test_rebuild_reaches_the_ingest_function(self, tmp_data_root, seen):
        backfill.backfill("day_aggs", START, END, rebuild=True, limit=1)
        assert seen == [(False, True)]

    def test_both_modes_raise_before_any_ingest(self, tmp_data_root, seen):
        with pytest.raises(ValueError, match="not set both"):
            backfill.backfill("day_aggs", START, END, refetch=True, rebuild=True)
        assert seen == []

    @pytest.mark.parametrize("flag,expected", [("--refetch", (True, False)),
                                               ("--rebuild", (False, True))])
    def test_the_cli_flag_reaches_the_ingest_function(
        self, tmp_data_root, seen, flag, expected
    ):
        assert backfill.main(["day_aggs", "2024-01-02", "2024-01-02", flag]) == 0
        assert seen == [expected]

    def test_the_cli_refuses_both_flags(self, tmp_data_root, seen):
        with pytest.raises(SystemExit):
            backfill.main(["day_aggs", "2024-01-02", "2024-01-02", "--refetch", "--rebuild"])
        assert seen == []

    def test_the_cli_refuses_force(self, tmp_data_root, seen):
        with pytest.raises(SystemExit):
            backfill.main(["day_aggs", "2024-01-02", "2024-01-02", "--force"])
        assert seen == []

    @pytest.mark.parametrize("target", sorted(backfill.TARGETS))
    def test_every_real_target_refuses_both_modes(self, tmp_data_root, target):
        with pytest.raises(ValueError, match="not set both"):
            backfill.TARGETS[target](SESSIONS[0], refetch=True, rebuild=True)

    def test_a_failure_in_a_mode_does_not_advise_a_plain_rerun(
        self, tmp_data_root, monkeypatch, capsys
    ):
        """A rerun with a mode processes every date again, not only the gaps."""

        def ingest(d, *, refetch=False, rebuild=False):
            raise FileNotFoundError(f"no vendor file for {d}")

        monkeypatch.setitem(backfill.TARGETS, "day_aggs", ingest)
        backfill.backfill("day_aggs", START, END, rebuild=True)
        out = capsys.readouterr().out
        assert "retry only these dates" not in out
        assert "each failed date as the range" in out


class TestRunLog:
    def _read_log(self, tmp_data_root) -> list[dict]:
        logs = list((tmp_data_root / "_logs").glob("backfill_day_aggs_*.jsonl"))
        assert len(logs) == 1, "the run must write exactly one log"
        return [json.loads(line) for line in logs[0].read_text().splitlines()]

    def test_the_log_holds_one_object_for_each_date(self, tmp_data_root, fake_ingest):
        fake_ingest()
        backfill.backfill("day_aggs", START, END)
        records = self._read_log(tmp_data_root)
        assert [r["date"] for r in records] == [d.isoformat() for d in SESSIONS]
        assert {r["status"] for r in records} == {"ok"}

    def test_the_log_records_the_error_of_a_failed_date(self, tmp_data_root, fake_ingest):
        fake_ingest(fail_on=(D(2024, 1, 4),))
        backfill.backfill("day_aggs", START, END)
        failed = [r for r in self._read_log(tmp_data_root) if r["status"] == "failed"]
        assert len(failed) == 1
        assert failed[0]["date"] == "2024-01-04"
        assert "vendor error" in failed[0]["error"]

    def test_a_none_return_is_skipped_and_not_published(self, tmp_data_root, fake_ingest):
        fake_ingest(skip_on=(D(2024, 1, 5),))
        backfill.backfill("day_aggs", START, END)
        records = self._read_log(tmp_data_root)
        by_date = {r["date"]: r["status"] for r in records}
        assert by_date["2024-01-05"] == "skipped"
        assert "path" not in next(r for r in records if r["date"] == "2024-01-05")

    def test_the_log_survives_a_run_that_fails_on_every_date(
        self, tmp_data_root, fake_ingest
    ):
        fake_ingest(fail_on=tuple(SESSIONS))
        backfill.backfill("day_aggs", START, END)
        assert len(self._read_log(tmp_data_root)) == len(SESSIONS)

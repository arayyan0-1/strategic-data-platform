"""Two fetches of the same date must not destroy each other's work.

This is not hypothetical. On 2026-08-23 a second copy of the backfill script ran
beside the first. Both fetched the same dates, both wrote to one shared temporary
name, and 288 dates failed: 247 because the loser of the rename found the file
already moved away, 41 because a reader caught the file mid-rename, and one on a
truncated read.

Nothing corrupt was published, because a vendor file only ever appears through an
atomic rename of a complete temporary file. The cost was a 36% failure rate on a
run of 1,255 dates.

These tests patch `paginate` and touch no network. What is under test is the file
handling and not the HTTP.
"""
import datetime as dt
import json
import threading
import time

import pytest

from sdp.ingest import rest

D = dt.date(2024, 1, 3)


def _records(n: int, delay: float = 0.0):
    def paginate(path, params):
        for i in range(n):
            if delay:
                time.sleep(delay)
            yield {"ticker": f"T{i}", "i": i}

    return paginate


def _parts(tmp_data_root, dataset="ds"):
    return sorted((tmp_data_root / "vendor" / dataset).glob("*.part"))


class TestTempName:
    def test_each_call_gets_a_different_name(self, tmp_path):
        dest = tmp_path / "2024-01-03.ndjson"
        a, b = rest._temp_beside(dest), rest._temp_beside(dest)
        assert a != b

    def test_the_temp_sits_beside_the_destination(self, tmp_path):
        """os.replace is atomic only inside one filesystem."""
        dest = tmp_path / "2024-01-03.ndjson"
        assert rest._temp_beside(dest).parent == dest.parent


class TestConcurrentWriters:
    def test_two_writers_for_one_date_both_succeed(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(rest, "paginate", _records(200, delay=0.0005))

        done, errors = [], []

        def run():
            try:
                done.append(rest.dump_ndjson("ds", "/p", {}, D))
            except Exception as exc:      # noqa: BLE001 -- the test reports it
                errors.append(exc)

        threads = [threading.Thread(target=run) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"a concurrent writer failed: {errors}"
        assert len(done) == 2
        assert done[0] == done[1], "both must name the same published file"

    def test_the_published_file_is_complete_after_a_race(self, tmp_data_root, monkeypatch):
        """The loser overwrites the winner. Either way the file is whole."""
        monkeypatch.setattr(rest, "paginate", _records(200, delay=0.0005))

        def run():
            rest.dump_ndjson("ds", "/p", {}, D)

        threads = [threading.Thread(target=run) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        dest = tmp_data_root / "vendor" / "ds" / "2024-01-03.ndjson"
        lines = dest.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 200
        assert [json.loads(line)["i"] for line in lines] == list(range(200))

    def test_a_race_leaves_no_temporary_file(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(rest, "paginate", _records(100, delay=0.0005))

        threads = [threading.Thread(target=rest.dump_ndjson,
                                    args=("ds", "/p", {}, D)) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert _parts(tmp_data_root) == []


class TestCleanup:
    def test_a_successful_write_leaves_no_temporary_file(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(rest, "paginate", _records(10))
        rest.dump_ndjson("ds", "/p", {}, D)
        assert _parts(tmp_data_root) == []

    def test_a_failure_part_way_through_leaves_no_temporary_file(
        self, tmp_data_root, monkeypatch
    ):
        """Otherwise every failed date leaves litter that the next run finds."""

        def explodes(path, params):
            yield {"ticker": "AAA"}
            raise RuntimeError("the vendor went away")

        monkeypatch.setattr(rest, "paginate", explodes)
        with pytest.raises(RuntimeError, match="went away"):
            rest.dump_ndjson("ds", "/p", {}, D)

        assert _parts(tmp_data_root) == []
        assert not (tmp_data_root / "vendor" / "ds" / "2024-01-03.ndjson").exists()

    def test_an_empty_answer_leaves_no_temporary_file(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(rest, "paginate", _records(0))
        assert rest.dump_ndjson("ds", "/p", {}, D, allow_empty=True) is None
        assert _parts(tmp_data_root) == []

    def test_an_empty_answer_without_the_flag_still_raises(self, tmp_data_root, monkeypatch):
        monkeypatch.setattr(rest, "paginate", _records(0))
        with pytest.raises(RuntimeError, match="no records"):
            rest.dump_ndjson("ds", "/p", {}, D)
        assert _parts(tmp_data_root) == []

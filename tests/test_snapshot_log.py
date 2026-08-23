"""The snapshot log for the current-state datasets.

raw/ stores splits and dividends as an append-only log. Each partition holds
the change of one pull. These tests state the contract:

- Replay of the log through a pull equals the full snapshot of that pull.
- A pull that changes nothing publishes a partition with zero rows.
- A later pull never changes what an earlier snapshot says.
- The log grows in pull order only. An out-of-order publish is refused.
- rebuild() replays vendor/ into the same log, so the log is recoverable.
"""
import datetime as dt
import gzip
import json

import duckdb
import pytest

from sdp import dal
from sdp.config import settings
from sdp.ingest import massive_corporate_actions as ca

D = dt.date
P1, P2, P3 = D(2026, 8, 9), D(2026, 8, 15), D(2026, 8, 22)

ROW_A = ("a", "AAA", D(2024, 1, 5), 0.5)
ROW_B = ("b", "BBB", D(2024, 3, 8), 0.25)


def _log_rows(ds, pull):
    rel = duckdb.connect().read_parquet(str(ds.partition_file(pull)))
    return rel.project("id, op").order("id, op").fetchall()


def _snapshot_ids(as_of):
    rel = dal.snapshot(dal.SPLITS, as_of)
    return sorted(r[0] for r in rel.project("id").fetchall())


class TestReplay:
    def test_the_first_pull_is_all_adds(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A, ROW_B])
        assert _log_rows(dal.SPLITS, P1) == [("a", "add"), ("b", "add")]

    def test_the_replay_equals_the_snapshot_that_was_appended(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A, ROW_B])
        ca_lake(dal.SPLITS, P2, [ROW_A, ("b", "BBB", D(2024, 3, 8), 0.125)])

        rows = dal.snapshot(dal.SPLITS, P2).project(
            "id, historical_adjustment_factor").order("id").fetchall()
        assert rows == [("a", 0.5), ("b", 0.125)]

    def test_an_unchanged_pull_publishes_zero_rows(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        ca_lake(dal.SPLITS, P2, [ROW_A])

        assert dal.partitions(dal.SPLITS) == [P1, P2]
        assert _log_rows(dal.SPLITS, P2) == []
        assert _snapshot_ids(P2) == ["a"]

    def test_a_restatement_is_one_close_and_one_add(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        ca_lake(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.4)])

        assert _log_rows(dal.SPLITS, P2) == [("a", "add"), ("a", "close")]

    def test_a_removed_row_is_one_close(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A, ROW_B])
        ca_lake(dal.SPLITS, P2, [ROW_A])

        assert _log_rows(dal.SPLITS, P2) == [("b", "close")]
        assert _snapshot_ids(P2) == ["a"]

    def test_a_later_pull_never_changes_an_earlier_snapshot(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        ca_lake(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.4), ROW_B])

        rows = dal.snapshot(dal.SPLITS, P1).project(
            "id, historical_adjustment_factor").order("id").fetchall()
        assert rows == [("a", 0.5)]

    def test_a_row_can_return_after_a_close(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        ca_lake(dal.SPLITS, P2, [])
        ca_lake(dal.SPLITS, P3, [ROW_A])

        assert _snapshot_ids(P2) == []
        assert _snapshot_ids(P3) == ["a"]

    def test_the_pull_date_of_a_row_is_the_pull_that_added_it(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        ca_lake(dal.SPLITS, P2, [ROW_A, ROW_B])

        rows = dal.snapshot(dal.SPLITS, P2).project(
            "id, pull_date").order("id").fetchall()
        assert rows == [("a", P1), ("b", P2)]

    def test_history_returns_the_log_with_the_op_column(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        ca_lake(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.4)])

        log = dal.history(dal.SPLITS)
        assert "op" in log.columns
        assert log.aggregate("count(*)").fetchall() == [(3,)]


class TestChainOrder:
    def test_an_out_of_order_publish_is_refused(self, ca_lake):
        ca_lake(dal.SPLITS, P2, [ROW_A])
        with pytest.raises(ca.ChainError, match="rebuild"):
            ca_lake(dal.SPLITS, P1, [ROW_A])

    def test_the_newest_pull_can_be_published_again_with_the_same_rows(self, ca_lake):
        ca_lake(dal.SPLITS, P1, [ROW_A])
        ca_lake(dal.SPLITS, P2, [ROW_A, ROW_B])
        before = _log_rows(dal.SPLITS, P2)

        ca_lake(dal.SPLITS, P2, [ROW_A, ROW_B])
        assert _log_rows(dal.SPLITS, P2) == before


def _vendor_record(id, ticker, factor):
    return {
        "id": id, "ticker": ticker, "execution_date": "2020-01-02",
        "split_from": 1.0, "split_to": 2.0,
        "adjustment_type": "forward_split",
        "historical_adjustment_factor": factor,
    }


class TestRebuild:
    """The recovery path. A doubt about one partition is a doubt about the chain."""

    def _write_vendor(self, records, name):
        path = settings.vendor_dir / "massive_splits" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        text = "".join(json.dumps(r) + "\n" for r in records)
        if name.endswith(".gz"):
            with gzip.open(path, "wt", encoding="utf-8") as fh:
                fh.write(text)
        else:
            path.write_text(text, encoding="utf-8")
        return path

    def _two_pulls(self):
        self._write_vendor(
            [_vendor_record("a", "AAA", 0.5), _vendor_record("b", "BBB", 0.5)],
            "2026-08-09.ndjson")
        self._write_vendor(
            [_vendor_record("a", "AAA", 0.4), _vendor_record("b", "BBB", 0.5),
             _vendor_record("c", "CCC", 0.5)],
            "2026-08-22.ndjson.gz")

    def test_rebuild_replays_every_vendor_pull_in_order(self, tmp_data_root):
        self._two_pulls()
        ca.rebuild("massive_splits")

        assert dal.partitions(dal.SPLITS) == [P1, P3]
        assert _snapshot_ids(P1) == ["a", "b"]
        assert _snapshot_ids(P3) == ["a", "b", "c"]
        assert _log_rows(dal.SPLITS, P3) == [
            ("a", "add"), ("a", "close"), ("c", "add")]

    def test_a_second_rebuild_gives_the_same_rows(self, tmp_data_root):
        self._two_pulls()
        ca.rebuild("massive_splits")
        first = [_log_rows(dal.SPLITS, p) for p in dal.partitions(dal.SPLITS)]

        ca.rebuild("massive_splits")
        second = [_log_rows(dal.SPLITS, p) for p in dal.partitions(dal.SPLITS)]
        assert first == second

    def test_rebuild_without_vendor_files_refuses(self, tmp_data_root):
        with pytest.raises(FileNotFoundError, match="vendor"):
            ca.rebuild("massive_splits")

"""The policy read for the historical window.

A current-state dataset has no history before the first pull. Every study of the
years before that pull must name the pull that it used. The choice must be
explicit at the call site, because a silent fallback to the newest pull is the
lookahead that the partition key exists to prevent.
"""
import datetime as dt

import pytest

from sdp import dal

D = dt.date


def test_snapshot_earliest_returns_the_first_pull(lake):
    lake(dal.SPLITS, D(2026, 8, 9), [("AAA", 1.0)])
    lake(dal.SPLITS, D(2026, 8, 15), [("AAA", 2.0)])
    lake(dal.SPLITS, D(2026, 8, 22), [("AAA", 3.0)])

    rel = dal.snapshot_earliest(dal.SPLITS)
    assert rel.project("pull_date").fetchall() == [(D(2026, 8, 9),)]


def test_snapshot_still_raises_before_the_first_pull(lake):
    """The earliest-pull policy is opt-in. It is never a silent fallback.

    If snapshot() answered with the oldest available pull, every historical read
    would quietly become a read of a later belief, and no error would say so.
    """
    lake(dal.SPLITS, D(2026, 8, 9), [("AAA", 1.0)])
    with pytest.raises(dal.MissingPartition, match="cannot backfill"):
        dal.snapshot(dal.SPLITS, D(2024, 3, 14))


def test_snapshot_earliest_refuses_an_event_stream(lake):
    lake(dal.DAY_AGGS, D(2026, 8, 9), [("AAA", 1.0)])
    with pytest.raises(ValueError, match="event stream"):
        dal.snapshot_earliest(dal.DAY_AGGS)


def test_snapshot_earliest_names_the_coverage_when_empty(tmp_data_root):
    with pytest.raises(dal.MissingPartition):
        dal.snapshot_earliest(dal.SPLITS)


def test_the_short_datasets_are_event_streams():
    """Short interest keys on the settlement date and it is backfillable.

    This is a different answer from splits and dividends, and the reason is in
    docs/decisions/0009. The endpoint takes the data date as a parameter, so the
    vendor can rebuild the answer for a past date.
    """
    assert dal.SHORT_VOLUME.key == "date"
    assert dal.SHORT_INTEREST.key == "date"
    with pytest.raises(ValueError, match="event stream"):
        dal.snapshot(dal.SHORT_INTEREST, D(2026, 8, 22))

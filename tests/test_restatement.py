"""The diff between two pulls.

The vendor id is not stable across pulls. A diff on the id reports hundreds of
deletions and insertions for events that did not change. These tests hold the
event-key diff in place, and they hold the treatment of the ambiguous key in
place, because a vendor contradiction inside one pull must never count as a
change between two pulls.
"""
import datetime as dt

import pytest

from sdp import dal, restatement

D = dt.date
P1, P2 = D(2026, 8, 9), D(2026, 8, 22)


def test_a_new_id_for_the_same_event_is_not_a_restatement(ca_lake):
    ca_lake(dal.SPLITS, P1, [("id-old", "AAA", D(2024, 1, 5), 0.5)])
    ca_lake(dal.SPLITS, P2, [("id-new", "AAA", D(2024, 1, 5), 0.5)])

    d = restatement.diff(dal.SPLITS)
    assert (d.ids_gone, d.ids_new) == (1, 1)
    assert (d.events_gone, d.events_new, d.events_restated) == (0, 0, 0)


def test_a_changed_factor_is_a_restatement(ca_lake):
    ca_lake(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5)])
    ca_lake(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.25)])

    d = restatement.diff(dal.SPLITS)
    assert d.events_restated == 1
    assert d.events_new == 0 and d.events_gone == 0
    assert d.max_relative_change == pytest.approx(0.5)
    assert d.restated_examples[0][:2] == ("AAA", D(2024, 1, 5))


def test_a_new_event_is_new_and_not_restated(ca_lake):
    ca_lake(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5)])
    ca_lake(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.5),
                             ("b", "BBB", D(2026, 8, 20), 0.1)])

    d = restatement.diff(dal.SPLITS)
    assert (d.events_new, d.events_restated) == (1, 0)


def test_an_ambiguous_key_is_reported_and_not_counted_as_a_change(ca_lake):
    """Two rows share a ticker and a date and they disagree about the factor.

    That is a contradiction inside one pull. It is not a change between two
    pulls, and counting it as one would make every pull look like a restatement.
    """
    ca_lake(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5),
                             ("b", "AAA", D(2024, 1, 5), 0.25)])
    ca_lake(dal.SPLITS, P2, [("c", "AAA", D(2024, 1, 5), 0.5),
                             ("d", "AAA", D(2024, 1, 5), 0.25)])

    d = restatement.diff(dal.SPLITS)
    assert (d.ambiguous_older, d.ambiguous_newer) == (1, 1)
    assert d.events_restated == 0


def test_a_null_factor_that_becomes_a_number_is_a_restatement(ca_lake):
    ca_lake(dal.DIVIDENDS, P1, [("a", "AAA", D(2024, 1, 5), None)])
    ca_lake(dal.DIVIDENDS, P2, [("a", "AAA", D(2024, 1, 5), 0.99)])

    d = restatement.diff(dal.DIVIDENDS)
    assert d.events_restated == 1


def test_a_diff_needs_two_pulls(ca_lake):
    ca_lake(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5)])
    with pytest.raises(dal.MissingPartition, match="needs two"):
        restatement.diff(dal.SPLITS)


def test_drift_bounds_the_earliest_pull_policy(ca_lake):
    """The error of the earliest-pull choice, over the events that a study uses."""
    ca_lake(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5),
                             ("b", "BBB", D(2024, 6, 5), 0.5)])
    ca_lake(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.4),
                             ("b", "BBB", D(2024, 6, 5), 0.5)])

    text = restatement.drift(dal.SPLITS, D(2024, 1, 1), D(2024, 12, 31))
    assert "matched events       2" in text
    assert "restated             1 (50.000 percent), 1 tickers" in text

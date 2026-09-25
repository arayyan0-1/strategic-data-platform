"""Diff between two vendor pulls.

The diff reads vendor/, not raw/. It uses the event key, not the vendor id
(which is not stable across pulls). A key whose rows disagree within one pull
is ambiguous. It is reported, not counted as a change.
"""
import datetime as dt
import json

import pytest

from sdp import dal, restatement
from sdp.config import settings

D = dt.date
P1, P2 = D(2026, 8, 9), D(2026, 8, 22)


@pytest.fixture
def ca_vendor(tmp_data_root):
    """Write one vendor pull. rows are (id, ticker, event_date, factor)."""

    def write(ds: dal.Dataset, pull: D, rows: list[tuple]):
        key = "execution_date" if ds is dal.SPLITS else "ex_dividend_date"
        path = settings.vendor_dir / ds.name / f"{pull:%Y-%m-%d}.ndjson"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(
            json.dumps({"id": i, "ticker": t, key: d.isoformat(),
                        "historical_adjustment_factor": f}) + "\n"
            for i, t, d, f in rows
        ), encoding="utf-8")
        return path

    return write


def test_a_new_id_for_the_same_event_is_not_a_restatement(ca_vendor):
    ca_vendor(dal.SPLITS, P1, [("id-old", "AAA", D(2024, 1, 5), 0.5)])
    ca_vendor(dal.SPLITS, P2, [("id-new", "AAA", D(2024, 1, 5), 0.5)])

    d = restatement.diff(dal.SPLITS)
    assert (d.ids_gone, d.ids_new) == (1, 1)
    assert (d.events_gone, d.events_new, d.events_restated) == (0, 0, 0)
    assert d.ids_churned == 1


def test_a_gone_event_is_not_id_churn(ca_vendor):
    """Churn counts ids whose event is still present. A gone event is not churn."""
    ca_vendor(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5),
                             ("b", "BBB", D(2024, 2, 5), 0.5)])
    ca_vendor(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.5)])

    d = restatement.diff(dal.SPLITS)
    assert (d.ids_gone, d.events_gone, d.ids_churned) == (1, 1, 0)
    assert "id churn only        0 ids" in str(d)


def test_a_changed_factor_is_a_restatement(ca_vendor):
    ca_vendor(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5)])
    ca_vendor(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.25)])

    d = restatement.diff(dal.SPLITS)
    assert d.events_restated == 1
    assert d.events_new == 0 and d.events_gone == 0
    assert d.max_relative_change == pytest.approx(0.5)
    assert d.restated_examples[0][:2] == ("AAA", D(2024, 1, 5))


def test_a_new_event_is_new_and_not_restated(ca_vendor):
    ca_vendor(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5)])
    ca_vendor(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.5),
                             ("b", "BBB", D(2026, 8, 20), 0.1)])

    d = restatement.diff(dal.SPLITS)
    assert (d.events_new, d.events_restated) == (1, 0)


def test_an_ambiguous_key_is_reported_and_not_counted_as_a_change(ca_vendor):
    """Two rows share a ticker and a date and they disagree about the factor.

    That is a contradiction inside one pull. It is not a change between two
    pulls, and counting it as one would make every pull look like a restatement.
    """
    ca_vendor(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5),
                             ("b", "AAA", D(2024, 1, 5), 0.25)])
    ca_vendor(dal.SPLITS, P2, [("c", "AAA", D(2024, 1, 5), 0.5),
                             ("d", "AAA", D(2024, 1, 5), 0.25)])

    d = restatement.diff(dal.SPLITS)
    assert (d.ambiguous_older, d.ambiguous_newer) == (1, 1)
    assert d.events_restated == 0


def test_a_null_factor_that_becomes_a_number_is_a_restatement(ca_vendor):
    ca_vendor(dal.DIVIDENDS, P1, [("a", "AAA", D(2024, 1, 5), None)])
    ca_vendor(dal.DIVIDENDS, P2, [("a", "AAA", D(2024, 1, 5), 0.99)])

    d = restatement.diff(dal.DIVIDENDS)
    assert d.events_restated == 1


def test_a_null_change_is_counted_apart_from_the_sizes(ca_vendor):
    """A change to or from null has no size, so the largest change must not hide it."""
    ca_vendor(dal.DIVIDENDS, P1, [("a", "AAA", D(2024, 1, 5), None),
                                ("b", "BBB", D(2024, 1, 5), 0.9)])
    ca_vendor(dal.DIVIDENDS, P2, [("a", "AAA", D(2024, 1, 5), 0.99),
                                ("b", "BBB", D(2024, 1, 5), None)])

    d = restatement.diff(dal.DIVIDENDS)
    assert (d.events_restated, d.events_null_changed) == (2, 2)
    assert d.max_relative_change is None
    text = restatement.drift(dal.DIVIDENDS, D(2024, 1, 1), D(2024, 12, 31))
    assert "to or from null      2" in text


def test_duplicate_rows_that_agree_are_compared(ca_vendor):
    """Two rows for one key with one factor are a duplicate, not a contradiction."""
    ca_vendor(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5),
                             ("b", "AAA", D(2024, 1, 5), 0.5)])
    ca_vendor(dal.SPLITS, P2, [("c", "AAA", D(2024, 1, 5), 0.4),
                             ("d", "AAA", D(2024, 1, 5), 0.4)])

    d = restatement.diff(dal.SPLITS)
    assert (d.ambiguous_older, d.ambiguous_newer) == (0, 0)
    assert d.events_restated == 1


def test_a_key_with_a_null_and_a_number_is_ambiguous(ca_vendor):
    ca_vendor(dal.DIVIDENDS, P1, [("a", "AAA", D(2024, 1, 5), None),
                                ("b", "AAA", D(2024, 1, 5), 0.9)])
    ca_vendor(dal.DIVIDENDS, P2, [("a", "AAA", D(2024, 1, 5), 0.9)])

    d = restatement.diff(dal.DIVIDENDS)
    assert (d.ambiguous_older, d.ambiguous_newer) == (1, 0)
    assert d.events_restated == 0


def test_a_diff_needs_two_pulls(ca_vendor):
    ca_vendor(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5)])
    with pytest.raises(FileNotFoundError, match="needs two"):
        restatement.diff(dal.SPLITS)


def test_drift_measures_the_movement_between_two_pulls(ca_vendor):
    """What separates the oldest kept pull from the newest, over a window."""
    ca_vendor(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5),
                             ("b", "BBB", D(2024, 6, 5), 0.5)])
    ca_vendor(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.4),
                             ("b", "BBB", D(2024, 6, 5), 0.5)])

    text = restatement.drift(dal.SPLITS, D(2024, 1, 1), D(2024, 12, 31))
    assert "matched events       2" in text
    assert "restated             1 (50.000 percent), 1 tickers" in text


def test_drift_can_start_from_a_named_pull(ca_vendor):
    """A study compares the pull it used with the newest pull."""
    P0 = D(2026, 8, 1)
    ca_vendor(dal.SPLITS, P0, [("a", "AAA", D(2024, 1, 5), 0.3)])
    ca_vendor(dal.SPLITS, P1, [("a", "AAA", D(2024, 1, 5), 0.5)])
    ca_vendor(dal.SPLITS, P2, [("a", "AAA", D(2024, 1, 5), 0.5)])

    text = restatement.drift(dal.SPLITS, D(2024, 1, 1), D(2024, 12, 31), older=P1)
    assert f"vendor pull {P1} against {P2}" in text
    assert "restated             0 (0.000 percent)" in text

"""Invariants of the audits.

An audit is SQL over a Parquet file. A test that writes a small Parquet file with
a known defect and checks the result is therefore a real test and not a mock.

Several of these checks exist because of a specific trap. Nothing protected them
from a later edit until now. The two that matter most:

- `split_from <= 0` is checked on its own, because DuckDB returns NULL for a
  division by zero and `NULL > 10000` is NULL. Without the separate check the
  ratio test passes and reports nothing.
- The meaning of a null adjustment factor is opposite in the two corporate action
  datasets. That asymmetry is deliberate and it is counterintuitive.

The audits compare against `current_date`, so every fixture uses a fixed past
date or a fixed future date. A relative date would make these tests depend on the
day they run.
"""
import datetime as dt

import duckdb
import pytest

from sdp.ingest import massive_corporate_actions as ca
from sdp.ingest import massive_day_aggs as day
from sdp.ingest import massive_tickers as tick

PAST = dt.date(2020, 1, 2)
FUTURE = dt.date(2099, 1, 2)
PULL = dt.date(2024, 1, 3)

SPLITS_DDL = """
create table t (
    id varchar, ticker varchar, execution_date date,
    split_from double, split_to double,
    adjustment_type varchar, historical_adjustment_factor double
)"""

DIVIDENDS_DDL = """
create table t (
    id varchar, ticker varchar, ex_dividend_date date,
    cash_amount double, historical_adjustment_factor double,
    currency varchar, frequency bigint
)"""


def _write(path, ddl, rows):
    con = duckdb.connect()
    con.execute(ddl)
    if rows:
        marks = ",".join("?" * len(rows[0]))
        con.executemany(f"insert into t values ({marks})", rows)
    con.execute(f"copy t to '{path}' (format parquet)")
    con.close()
    return path


def split_row(id="s1", ticker="AAA", execution_date=PAST, split_from=1.0,
              split_to=2.0, adjustment_type="forward_split", factor=0.5):
    return (id, ticker, execution_date, split_from, split_to, adjustment_type, factor)


def dividend_row(id="d1", ticker="AAA", ex_dividend_date=PAST, cash_amount=0.25,
                 factor=0.99, currency="USD", frequency=4):
    return (id, ticker, ex_dividend_date, cash_amount, factor, currency, frequency)


@pytest.fixture
def splits(tmp_path):
    return lambda rows, name="s.parquet": _write(tmp_path / name, SPLITS_DDL, rows)


@pytest.fixture
def dividends(tmp_path):
    return lambda rows, name="d.parquet": _write(tmp_path / name, DIVIDENDS_DDL, rows)


class TestSplitAudit:
    def test_a_valid_file_passes(self, splits):
        ca._audit_splits(splits([split_row()]))

    def test_split_from_of_zero_is_fatal(self, splits):
        """The check that exists because NULL > 10000 is NULL, not false."""
        with pytest.raises(ca.AuditFailure, match="invalid split ratio"):
            ca._audit_splits(splits([split_row(split_from=0.0)]))

    def test_a_negative_split_from_is_fatal(self, splits):
        with pytest.raises(ca.AuditFailure, match="invalid split ratio"):
            ca._audit_splits(splits([split_row(split_from=-1.0)]))

    def test_a_null_execution_date_is_fatal(self, splits):
        with pytest.raises(ca.AuditFailure, match="execution_date"):
            ca._audit_splits(splits([split_row(execution_date=None)]))

    def test_a_null_factor_on_an_executed_split_is_fatal(self, splits):
        """The split factor is mechanical. A null means that something broke."""
        with pytest.raises(ca.AuditFailure, match="null factors"):
            ca._audit_splits(splits([split_row(factor=None)]))

    def test_a_null_factor_on_a_pending_split_is_not_fatal(self, splits):
        """The vendor gives no factor before the event executes."""
        ca._audit_splits(splits([split_row(execution_date=FUTURE, factor=None)]))

    @pytest.mark.parametrize(
        "adjustment_type,split_from,split_to",
        [("forward_split", 2.0, 1.0), ("reverse_split", 1.0, 2.0),
         ("stock_dividend", 2.0, 1.0)],
    )
    def test_a_ratio_in_the_wrong_direction_is_fatal(
        self, splits, adjustment_type, split_from, split_to
    ):
        row = split_row(adjustment_type=adjustment_type,
                        split_from=split_from, split_to=split_to)
        with pytest.raises(ca.AuditFailure, match="wrong direction"):
            ca._audit_splits(splits([row]))

    def test_an_extreme_ratio_warns_and_does_not_raise(self, splits):
        """NPWZ and DAVL are real rows. The fatal list stays narrow."""
        ca._audit_splits(splits([split_row(split_from=1.0, split_to=2_000_000.0)]))

    def test_a_zero_factor_warns_and_does_not_raise(self, splits):
        """RYCEF is one bad row out of 3,722 stock dividends."""
        ca._audit_splits(splits([split_row(factor=0.0)]))


class TestDividendAudit:
    def test_a_valid_file_passes(self, dividends):
        ca._audit_dividends(dividends([dividend_row()]))

    def test_a_null_factor_on_a_past_ex_date_is_not_fatal(self, dividends):
        """The opposite of the split rule, and deliberate.

        The dividend factor needs a price on the ex-date. A null therefore means
        that the vendor has no price for that security. That is structural.
        """
        ca._audit_dividends(dividends([dividend_row(factor=None)]))

    def test_a_null_cash_amount_is_fatal(self, dividends):
        """Three-valued logic. The predicate must test for null on its own.

        A rewrite to `cash_amount < 0` alone passes every other test here and
        fails this one, because NULL < 0 is NULL and not true.
        """
        with pytest.raises(ca.AuditFailure, match="cash_amount"):
            ca._audit_dividends(dividends([dividend_row(cash_amount=None)]))

    def test_a_negative_cash_amount_is_fatal(self, dividends):
        with pytest.raises(ca.AuditFailure, match="cash_amount"):
            ca._audit_dividends(dividends([dividend_row(cash_amount=-1.0)]))

    def test_a_null_ex_dividend_date_is_fatal(self, dividends):
        with pytest.raises(ca.AuditFailure, match="ex_dividend_date"):
            ca._audit_dividends(dividends([dividend_row(ex_dividend_date=None)]))

    def test_a_row_that_is_not_in_usd_warns_and_does_not_raise(self, dividends):
        ca._audit_dividends(dividends([dividend_row(currency="CAD")]))


class TestCommonAudit:
    def test_a_valid_file_returns_the_row_count(self, splits):
        assert ca._audit_common(splits([split_row(), split_row(id="s2")]),
                                "massive_splits") == 2

    def test_zero_rows_is_fatal(self, splits):
        with pytest.raises(ca.AuditFailure, match="zero rows"):
            ca._audit_common(splits([]), "massive_splits")

    def test_a_duplicate_id_is_fatal(self, splits):
        rows = [split_row(id="same"), split_row(id="same", ticker="BBB")]
        with pytest.raises(ca.AuditFailure, match="duplicate ids"):
            ca._audit_common(splits(rows), "massive_splits")

    def test_a_null_id_is_fatal(self, splits):
        with pytest.raises(ca.AuditFailure, match="null ids"):
            ca._audit_common(splits([split_row(id=None)]), "massive_splits")

    def test_a_null_ticker_is_fatal(self, splits):
        with pytest.raises(ca.AuditFailure, match="null tickers"):
            ca._audit_common(splits([split_row(ticker=None)]), "massive_splits")


class TestDeltaAudit:
    """The sharp check. An absolute threshold on a growing dataset goes blunt."""

    def _publish_prior(self, tmp_data_root, rows, pull_date=dt.date(2024, 1, 2)):
        path = ca.raw_path("massive_splits", pull_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        return _write(path, SPLITS_DDL, rows)

    def test_the_first_pull_has_no_baseline_and_passes(self, tmp_data_root, splits):
        ca._audit_vs_previous("massive_splits", splits([split_row()]), PULL, 1)

    def test_more_new_defective_tickers_than_the_threshold_is_fatal(
        self, tmp_data_root, splits
    ):
        self._publish_prior(tmp_data_root, [split_row()])
        bad = [split_row(id=f"s{i}", ticker=f"BAD{i}", factor=0.0)
               for i in range(ca.MAX_NEW_BAD_TICKERS + 1)]
        with pytest.raises(ca.AuditFailure, match="became defective"):
            ca._audit_vs_previous("massive_splits", splits(bad + [split_row()]),
                                  PULL, len(bad) + 1)

    def test_exactly_the_threshold_warns_and_does_not_raise(self, tmp_data_root, splits):
        self._publish_prior(tmp_data_root, [split_row()])
        bad = [split_row(id=f"s{i}", ticker=f"BAD{i}", factor=0.0)
               for i in range(ca.MAX_NEW_BAD_TICKERS)]
        ca._audit_vs_previous("massive_splits", splits(bad + [split_row()]),
                              PULL, len(bad) + 1)

    def test_a_ticker_that_was_already_defective_is_not_new(self, tmp_data_root, splits):
        """This is why the delta check stays sharp. FSFF is static."""
        known_bad = [split_row(id=f"s{i}", ticker=f"BAD{i}", factor=0.0)
                     for i in range(10)]
        self._publish_prior(tmp_data_root, known_bad)
        ca._audit_vs_previous("massive_splits", splits(known_bad), PULL, len(known_bad))

    def test_a_history_that_shrank_is_fatal(self, tmp_data_root, splits):
        prior = [split_row(id=f"s{i}", ticker=f"T{i}") for i in range(100)]
        self._publish_prior(tmp_data_root, prior)
        with pytest.raises(ca.AuditFailure, match="became smaller"):
            ca._audit_vs_previous("massive_splits", splits([split_row()]), PULL, 1)


def _write_day_aggs(path, n=6000, extra=""):
    duckdb.execute(f"""
        copy (
            select 'T' || i as ticker, date '2024-01-03' as date,
                   10.0 as open, 11.0 as high, 9.0 as low, 10.5 as close,
                   1000.0 as volume, 5 as transactions,
                   now() as window_start_utc
            from range({n}) t(i)
            {extra}
        ) to '{path}' (format parquet)
    """)
    return path


class TestDayAggsAudit:
    def test_a_valid_session_passes(self, tmp_path):
        day.audit(_write_day_aggs(tmp_path / "a.parquet"))

    def test_too_few_rows_is_fatal(self, tmp_path):
        with pytest.raises(day.AuditFailure, match="full session"):
            day.audit(_write_day_aggs(tmp_path / "a.parquet", n=100))

    def test_a_high_below_the_low_is_fatal(self, tmp_path):
        extra = ("union all select 'BAD', date '2024-01-03', 10.0, 1.0, 9.0, "
                 "10.5, 1000.0, 5, now()")
        with pytest.raises(day.AuditFailure, match="high < low"):
            day.audit(_write_day_aggs(tmp_path / "a.parquet", extra=extra))

    def test_a_duplicate_ticker_is_fatal(self, tmp_path):
        extra = ("union all select 'T1', date '2024-01-03', 10.0, 11.0, 9.0, "
                 "10.5, 1000.0, 5, now()")
        with pytest.raises(day.AuditFailure, match="duplicate tickers"):
            day.audit(_write_day_aggs(tmp_path / "a.parquet", extra=extra))

    def test_a_non_positive_price_is_fatal(self, tmp_path):
        extra = ("union all select 'BAD', date '2024-01-03', 0.0, 11.0, 0.0, "
                 "0.0, 1000.0, 5, now()")
        with pytest.raises(day.AuditFailure, match="non-positive price"):
            day.audit(_write_day_aggs(tmp_path / "a.parquet", extra=extra))


def _write_tickers(path, n=6000, n_cs=4000, extra=""):
    duckdb.execute(f"""
        copy (
            select 'T' || i as ticker,
                   case when i < {n_cs} then 'CS' else 'ETF' end as type,
                   'XNYS' as primary_exchange,
                   'BBG' || i as composite_figi,
                   true as active
            from range({n}) t(i)
            {extra}
        ) to '{path}' (format parquet)
    """)
    return path


class TestTickersAudit:
    def test_a_valid_universe_passes(self, tmp_path):
        assert tick.audit(_write_tickers(tmp_path / "t.parquet"), PULL) == 6000

    def test_too_few_rows_is_fatal(self, tmp_path):
        with pytest.raises(tick.AuditFailure, match="outside the limits"):
            tick.audit(_write_tickers(tmp_path / "t.parquet", n=100, n_cs=50), PULL)

    def test_too_few_common_stock_rows_is_fatal(self, tmp_path):
        with pytest.raises(tick.AuditFailure, match="common stock"):
            tick.audit(_write_tickers(tmp_path / "t.parquet", n_cs=10), PULL)

    def test_an_inactive_row_is_fatal(self, tmp_path):
        """Ingest requests active=true. An inactive row means a wrong request."""
        extra = "union all select 'DEAD', 'CS', 'XNYS', 'BBGDEAD', false"
        with pytest.raises(tick.AuditFailure, match="active=true"):
            tick.audit(_write_tickers(tmp_path / "t.parquet", extra=extra), PULL)

    def test_a_duplicate_ticker_is_fatal(self, tmp_path):
        extra = "union all select 'T1', 'CS', 'XNYS', 'BBG1', true"
        with pytest.raises(tick.AuditFailure, match="duplicate tickers"):
            tick.audit(_write_tickers(tmp_path / "t.parquet", extra=extra), PULL)

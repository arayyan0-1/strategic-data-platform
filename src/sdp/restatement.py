"""Measure what changed between two vendor pulls of a corporate action dataset.
raw/ holds one table per dataset, replaced by each pull, so it keeps no history.
vendor/ does, and this module reads that archive, not raw/. Diff on the event
key, never the vendor id, which is not stable across pulls. A key that is
ambiguous within a pull is reported, not counted as a change.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

import duckdb

from sdp import dal
from sdp.ingest import massive_corporate_actions as ca

# The event key of each current-state dataset.
KEYS: dict[str, str] = {
    dal.SPLITS.name: "execution_date",
    dal.DIVIDENDS.name: "ex_dividend_date",
}


@dataclass
class Diff:
    dataset: str
    older: dt.date
    newer: dt.date
    rows_older: int
    rows_newer: int
    ids_gone: int
    ids_new: int
    events_new: int
    events_gone: int
    events_restated: int
    ambiguous_older: int
    ambiguous_newer: int
    max_relative_change: float | None
    ids_churned: int = 0
    """Ids that are gone while their event is still in the newer pull."""
    restated_examples: list[tuple] = field(default_factory=list)

    def __str__(self) -> str:
        return "\n".join([
            f"{self.dataset}: {self.older} -> {self.newer}",
            f"  rows                 {self.rows_older} -> {self.rows_newer}",
            f"  id diff              {self.ids_gone} gone, {self.ids_new} new",
            f"  event diff           {self.events_gone} gone, {self.events_new} new",
            f"  restated factors     {self.events_restated}",
            f"  id churn only        {self.ids_churned} ids gone, event still present",
            f"  ambiguous keys       {self.ambiguous_older} -> {self.ambiguous_newer}",
            f"  largest change       {self.max_relative_change}",
        ])


def _pull_dates(ds: dal.Dataset) -> list[dt.date]:
    """Return the dates of the vendor pulls that are kept for this dataset."""
    pulls = [d for d, _ in ca.vendor_pulls(ds.name)]
    if len(pulls) < 2:
        raise FileNotFoundError(
            f"{ds.name} has {len(pulls)} vendor pull(s). A diff needs two. Run "
            f"python -m sdp.daily on more than one day."
        )
    return pulls


def _key(ds: dal.Dataset) -> str:
    """Return the event key column. Raise for a dataset that is not current state."""
    if ds.name not in KEYS:
        raise ValueError(f"{ds.name} is not a current-state dataset.")
    return KEYS[ds.name]


def _register(con, name: str, ds: dal.Dataset, pull: dt.date) -> None:
    """Load one vendor pull into a temp table under the given name. The queries
    read each pull more than once, so the file is parsed one time only."""
    path = dict(ca.vendor_pulls(ds.name)).get(pull)
    if path is None:
        raise FileNotFoundError(f"{ds.name} has no vendor file for the pull date {pull}.")
    # DuckDB reads gzip NDJSON directly. The cast is defensive: an all-null
    # factor column infers a non-arithmetic type and would fail the comparison.
    con.execute(
        f"create or replace temp table {name} as select * replace ("
        f"cast(historical_adjustment_factor as double) "
        f"as historical_adjustment_factor) from "
        f"read_json('{path}', format='newline_delimited', sample_size=-1)")


def _keyed(table: str, key: str, window: tuple[dt.date, dt.date] | None = None) -> str:
    """Return SQL with one row for each event key of a loaded pull: ticker, ev,
    n_rows, n_f and f. A window keeps only the event dates in that closed range."""
    where = ""
    if window is not None:
        start, end = window
        where = f"where {key} between date '{start}' and date '{end}'"
    return f"""
        select ticker, {key} as ev,
               count(*) as n_rows,
               count(distinct historical_adjustment_factor) as n_f,
               min(historical_adjustment_factor) as f
        from {table} {where}
        group by 1, 2
    """


def diff(ds: dal.Dataset, older: dt.date | None = None,
         newer: dt.date | None = None, *, examples: int = 5) -> Diff:
    """Compare two pulls on the event key."""
    key = _key(ds)
    parts = _pull_dates(ds)
    older = older or parts[0]
    newer = newer or parts[-1]
    if older >= newer:
        raise ValueError(f"The older pull {older} is not before the newer {newer}.")

    keyed = (f"ok as ({_keyed('_ca_older', key)}), "
             f"nk as ({_keyed('_ca_newer', key)})")
    con = duckdb.connect()
    try:
        _register(con, "_ca_older", ds, older)
        _register(con, "_ca_newer", ds, newer)
        # An anti join does not match a null, and one null cannot hide the other
        # rows as it does with NOT IN.
        row = con.execute(f"""
            with {keyed}
            select
                (select count(*) from _ca_older),
                (select count(*) from _ca_newer),
                (select count(*) from _ca_older anti join _ca_newer using (id)),
                (select count(*) from _ca_newer anti join _ca_older using (id)),
                (select count(*) from nk anti join ok using (ticker, ev)),
                (select count(*) from ok anti join nk using (ticker, ev)),
                (select count(*) from ok join nk using (ticker, ev)
                    where ok.n_rows = 1 and nk.n_rows = 1
                      and ok.f is distinct from nk.f),
                (select count(*) from ok where n_rows > 1),
                (select count(*) from nk where n_rows > 1),
                (select max(abs(nk.f - ok.f) / nullif(abs(ok.f), 0))
                    from ok join nk using (ticker, ev)
                    where ok.n_rows = 1 and nk.n_rows = 1
                      and ok.f is distinct from nk.f),
                (select count(*) from _ca_older g
                    where not exists (select 1 from _ca_newer n where n.id = g.id)
                      and exists (select 1 from _ca_newer n
                                  where n.ticker = g.ticker and n.{key} = g.{key}))
        """).fetchone()
        if row is None:
            raise RuntimeError(f"The diff query for {ds.name} did not return a row.")
        sample = con.execute(f"""
            with {keyed}
            select ok.ticker, ok.ev, ok.f as was, nk.f as now
            from ok join nk using (ticker, ev)
            where ok.n_rows = 1 and nk.n_rows = 1 and ok.f is distinct from nk.f
            order by abs(nk.f - ok.f) / nullif(abs(ok.f), 0) desc, ok.ticker, ok.ev
            limit {examples}
        """).fetchall()
    finally:
        con.close()

    return Diff(ds.name, older, newer, *row, restated_examples=sample)


def drift(ds: dal.Dataset, start: dt.date, end: dt.date, *,
          older: dt.date | None = None, newer: dt.date | None = None) -> str:
    """Vendor drift between two pulls, over a date window. The default pulls are
    the oldest and the newest. A study names the pull it used as older. Reads the
    vendor archive."""
    key = _key(ds)
    parts = _pull_dates(ds)
    older = older or parts[0]
    newer = newer or parts[-1]
    if older >= newer:
        raise ValueError(f"The older pull {older} is not before the newer {newer}.")
    window = (start, end)
    con = duckdb.connect()
    try:
        _register(con, "_ca_older", ds, older)
        _register(con, "_ca_newer", ds, newer)
        row = con.execute(f"""
            with ok as ({_keyed('_ca_older', key, window)}),
                 nk as ({_keyed('_ca_newer', key, window)})
            select count(*),
                   count(*) filter (ok.f is distinct from nk.f),
                   count(distinct ok.ticker) filter (ok.f is distinct from nk.f),
                   median(abs(nk.f - ok.f) / nullif(abs(ok.f), 0))
                       filter (ok.f is distinct from nk.f),
                   max(abs(nk.f - ok.f) / nullif(abs(ok.f), 0))
            from ok join nk using (ticker, ev)
            where ok.n_rows = 1 and nk.n_rows = 1
        """).fetchone()
    finally:
        con.close()
    if row is None:
        raise RuntimeError(f"The drift query for {ds.name} did not return a row.")
    n, changed, tickers, med, worst = row
    pct = (100.0 * changed / n) if n else 0.0
    return (
        f"{ds.name} drift, vendor pull {older} against {newer}, "
        f"events from {start} to {end}\n"
        f"  matched events       {n}\n"
        f"  restated             {changed} ({pct:.3f} percent), "
        f"{tickers} tickers\n"
        f"  median change        {med}\n"
        f"  largest change       {worst}"
    )


def report() -> str:
    out = []
    for ds in (dal.SPLITS, dal.DIVIDENDS):
        try:
            out.append(str(diff(ds)))
        except (FileNotFoundError, ValueError) as exc:
            out.append(f"{ds.name}: {exc}")
    return "\n\n".join(out)


if __name__ == "__main__":
    print(report())

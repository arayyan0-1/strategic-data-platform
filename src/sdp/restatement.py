"""Measure what changed between two vendor pulls of a corporate action dataset.

raw/ holds one table for each of splits and dividends, replaced by every pull,
so the published lake has no record of what an earlier pull said. vendor/ does:
ingest keeps every pull, dated, byte for byte. This module reads that archive.

It is therefore a tool over vendor/ and not a read of the published lake. The
rule that every read of raw/ goes through sdp.dal is unaffected.

Read the diff on the event key and never on the vendor id. The id is not stable
across pulls. A diff on the id reports hundreds of deletions and insertions for
events that did not change, because the vendor regenerates the id.

The event key is not unique either. Two vendor rows can share a ticker and a
date, and they can disagree about the factor. See docs/decisions/0010. This
module reports those rows as 'ambiguous' and keeps them out of the restatement
count, so that a vendor contradiction inside one pull is never counted as a
change between two pulls.
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
    restated_examples: list[tuple] = field(default_factory=list)

    def __str__(self) -> str:
        churn = self.ids_gone - self.events_gone
        return "\n".join([
            f"{self.dataset}: {self.older} -> {self.newer}",
            f"  rows                 {self.rows_older} -> {self.rows_newer}",
            f"  id diff              {self.ids_gone} gone, {self.ids_new} new",
            f"  event diff           {self.events_gone} gone, {self.events_new} new",
            f"  restated factors     {self.events_restated}",
            f"  id churn only        {churn} events under a new id, unchanged",
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


def _register(con, name: str, ds: dal.Dataset, pull: dt.date) -> None:
    """Register one vendor pull as a view under the given name."""
    path = dict(ca.vendor_pulls(ds.name))[pull]
    # DuckDB reads gzip NDJSON directly, so no file is expanded on disk.
    #
    # The cast is defensive. read_json infers the type of each column, and a
    # pull whose factors are all null gives that column a type with no
    # arithmetic. The comparison below would then fail on a binder error
    # instead of reporting a restatement.
    con.execute(
        f"create or replace temp view {name} as select * replace ("
        f"cast(historical_adjustment_factor as double) "
        f"as historical_adjustment_factor) from "
        f"read_json('{path}', format='newline_delimited', sample_size=-1)")


def diff(ds: dal.Dataset, older: dt.date | None = None,
         newer: dt.date | None = None, *, examples: int = 5) -> Diff:
    """Compare two pulls on the event key."""
    if ds.name not in KEYS:
        raise ValueError(f"{ds.name} is not a current-state dataset.")
    key = KEYS[ds.name]
    parts = _pull_dates(ds)
    older = older or parts[0]
    newer = newer or parts[-1]
    if older >= newer:
        raise ValueError(f"The older pull {older} is not before the newer {newer}.")

    con = duckdb.connect()
    _register(con, "_ca_older", ds, older)
    _register(con, "_ca_newer", ds, newer)

    sql = f"""
        with o as (select * from _ca_older),
             n as (select * from _ca_newer),
             ok as (select ticker, {key} as ev,
                           count(*) as n_rows,
                           count(distinct historical_adjustment_factor) as n_f,
                           min(historical_adjustment_factor) as f
                    from o group by 1, 2),
             nk as (select ticker, {key} as ev,
                           count(*) as n_rows,
                           count(distinct historical_adjustment_factor) as n_f,
                           min(historical_adjustment_factor) as f
                    from n group by 1, 2)
        select
            (select count(*) from o),
            (select count(*) from n),
            (select count(*) from o where id not in (select id from n)),
            (select count(*) from n where id not in (select id from o)),
            (select count(*) from nk where (ticker, ev) not in
                (select ticker, ev from ok)),
            (select count(*) from ok where (ticker, ev) not in
                (select ticker, ev from nk)),
            (select count(*) from ok join nk using (ticker, ev)
                where ok.n_rows = 1 and nk.n_rows = 1
                  and ok.f is distinct from nk.f),
            (select count(*) from ok where n_rows > 1),
            (select count(*) from nk where n_rows > 1),
            (select max(abs(nk.f - ok.f) / nullif(abs(ok.f), 0))
                from ok join nk using (ticker, ev)
                where ok.n_rows = 1 and nk.n_rows = 1
                  and ok.f is distinct from nk.f)
    """
    row = con.execute(sql).fetchone()
    assert row is not None

    sample = con.execute(f"""
        with o as (select * from _ca_older),
             n as (select * from _ca_newer),
             ok as (select ticker, {key} as ev, count(*) n_rows,
                           min(historical_adjustment_factor) f from o group by 1, 2),
             nk as (select ticker, {key} as ev, count(*) n_rows,
                           min(historical_adjustment_factor) f from n group by 1, 2)
        select ok.ticker, ok.ev, ok.f as was, nk.f as now
        from ok join nk using (ticker, ev)
        where ok.n_rows = 1 and nk.n_rows = 1 and ok.f is distinct from nk.f
        order by abs(nk.f - ok.f) / nullif(abs(ok.f), 0) desc
        limit {examples}
    """).fetchall()
    con.close()

    return Diff(ds.name, older, newer, *row, restated_examples=sample)


def drift(ds: dal.Dataset, start: dt.date, end: dt.date) -> str:
    """Measure how far the vendor moved between the oldest and newest pull.

    raw/ holds the belief of the vendor now, so a rebuild of the lake changes
    the adjusted prices whenever the vendor restates a factor. This function
    puts a number on that movement over the event dates that a study consumes,
    which is what a writeup needs to quote. It reads the vendor archive, so it
    keeps working however raw/ is stored.
    """
    key = KEYS[ds.name]
    parts = _pull_dates(ds)
    con = duckdb.connect()
    _register(con, "_ca_older", ds, parts[0])
    _register(con, "_ca_newer", ds, parts[-1])
    row = con.execute(f"""
        with o as (select ticker, {key} as ev, count(*) n_rows,
                          min(historical_adjustment_factor) f
                   from _ca_older
                   where {key} between date '{start}' and date '{end}'
                   group by 1, 2),
             n as (select ticker, {key} as ev, count(*) n_rows,
                          min(historical_adjustment_factor) f
                   from _ca_newer
                   where {key} between date '{start}' and date '{end}'
                   group by 1, 2)
        select count(*),
               count(*) filter (o.f is distinct from n.f),
               count(distinct o.ticker) filter (o.f is distinct from n.f),
               median(abs(n.f - o.f) / nullif(abs(o.f), 0))
                   filter (o.f is distinct from n.f),
               max(abs(n.f - o.f) / nullif(abs(o.f), 0))
        from o join n using (ticker, ev)
        where o.n_rows = 1 and n.n_rows = 1
    """).fetchone()
    con.close()
    assert row is not None
    n, changed, tickers, med, worst = row
    pct = (100.0 * changed / n) if n else 0.0
    return (
        f"{ds.name} drift, vendor pull {parts[0]} against {parts[-1]}, "
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

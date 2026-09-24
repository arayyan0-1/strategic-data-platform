# src/sdp/diagnostics.py
"""Measurements that close the open questions. Each function answers one question
with a number. Run after the backfill. Every read goes through sdp.dal.

    python -m sdp.diagnostics
"""
from __future__ import annotations

import datetime as dt

from sdp import dal

# A universe proxy so a diagnostic needs no dbt run. The dbt model is the
# definition, and the two can disagree.
MIN_PRICE = 5.0
MIN_ADV = 1_000_000.0
ADV_WINDOW = 20


def _sql(query: str, **relations) -> list[tuple]:
    con = dal.con()
    for name, rel in relations.items():
        con.register(name, rel)
    try:
        return con.execute(query).fetchall()
    finally:
        for name in relations:
            con.unregister(name)


def _liquid_bars_sql() -> str:
    """A trailing liquidity screen that ends at D-1, as a CTE body."""
    return f"""
        select ticker, date, close,
               avg(close * volume) over (
                   partition by ticker order by date
                   rows between {ADV_WINDOW} preceding and 1 preceding
               ) as adv
        from _bars
    """


# ---------- the identifier ----------

def figi_reuse() -> str:
    """How many CS tickers map to more than one composite_figi in the window? A
    freed-then-reused ticker would join two companies into one fat-tailed series,
    so a large count means a ticker key is not defensible."""
    rows = _sql("""
        with cs as (
            select ticker, composite_figi, share_class_figi, date
            from _tickers where type = 'CS'
        ),
        reused as (
            select ticker,
                   count(distinct composite_figi) as n_figi,
                   min(date) as first_seen, max(date) as last_seen
            from cs where composite_figi is not null
            group by ticker having count(distinct composite_figi) > 1
        )
        select (select count(distinct ticker) from cs),
               (select count(*) from reused),
               (select count(*) from reused where n_figi > 2)
    """, _tickers=dal.tickers())
    total, reused, many = rows[0]
    return (f"CS tickers in the window      {total}\n"
            f"  with more than one FIGI     {reused}\n"
            f"  with more than two FIGIs    {many}")


def figi_exposure() -> str:
    """What share of the traded universe falls back off FIGI? Reports the raw CS
    null rate and the rate after the liquidity filter."""
    rows = _sql(f"""
        with liquid as ({_liquid_bars_sql()}),
        screened as (
            select ticker, date from liquid
            where close >= {MIN_PRICE} and adv >= {MIN_ADV}
        ),
        cs as (
            select ticker, date, share_class_figi, composite_figi, cik
            from _tickers where type = 'CS'
        )
        select
            (select count(*) from cs),
            (select count(*) from cs where share_class_figi is null),
            (select count(*) from cs join screened using (ticker, date)),
            (select count(*) from cs join screened using (ticker, date)
                where share_class_figi is null),
            (select count(*) from cs join screened using (ticker, date)
                where share_class_figi is null and nullif(cik, '') is null)
    """, _tickers=dal.tickers(), _bars=dal.day_aggs())
    cs_rows, cs_null, scr_rows, scr_null, scr_no_key = rows[0]
    pct = lambda a, b: f"{100.0 * a / b:.2f}" if b else "n/a"  # noqa: E731
    return (f"CS rows                       {cs_rows}, "
            f"{pct(cs_null, cs_rows)} percent with no share_class_figi\n"
            f"CS rows after the screen      {scr_rows}, "
            f"{pct(scr_null, scr_rows)} percent with no share_class_figi\n"
            f"  and with no CIK either      {scr_no_key}")


# ---------- the ambiguous event key ----------

def corporate_action_duplicates() -> str:
    """How many events share a ticker and date and disagree about the factor? An
    as-of join against the raw rows would duplicate price rows. The staging model
    collapses them first, and this is the exposure it hides."""
    # The window is the first to the last day-aggregate session in the lake.
    coverage = dal.coverage(dal.DAY_AGGS)
    in_window_sql = (
        f"(select count(*) from k where n > 1 "
        f"and ev between date '{coverage[0]}' and date '{coverage[1]}')"
        if coverage else "null"
    )
    out = []
    for ds, key in ((dal.SPLITS, "execution_date"),
                    (dal.DIVIDENDS, "ex_dividend_date")):
        rel = dal.current(ds)
        rows = _sql(f"""
            with k as (
                select ticker, {key} as ev, count(*) as n,
                       count(distinct historical_adjustment_factor) as n_f
                from _ca group by 1, 2
            )
            select (select count(*) from k),
                   (select count(*) from k where n > 1),
                   (select count(*) from k where n_f > 1),
                   {in_window_sql}
        """, _ca=rel)
        total, dup, disagree, in_window = rows[0]
        if in_window is None:
            in_window = "not available"
        out.append(f"{ds.name}\n"
                   f"  distinct (ticker, date)     {total}\n"
                   f"  with more than one row      {dup}\n"
                   f"  that disagree on the factor {disagree}\n"
                   f"  with a date in the window   {in_window}")
    return "\n".join(out)


# ---------- the null dividend factor ----------

def dividend_factor_nulls() -> str:
    """Is the null dividend factor structural or a defect? The factor needs a
    price on the ex-date, so a null means the vendor has no price. The deciding
    check is the null rate inside the traded tape against outside it, over the
    whole bar history (a short window would count early delistings as untraded)."""
    rows = _sql("""
        with traded as (select distinct ticker from _bars),
        d as (select ticker, historical_adjustment_factor as f,
                     ticker in (select ticker from traded) as in_bars
              from _divs)
        select in_bars, count(*), count(*) filter (f is null)
        from d group by 1 order by 1
    """, _divs=dal.current(dal.DIVIDENDS), _bars=dal.day_aggs())
    lines = ["dividend rows, by whether the ticker trades on the ingested tape"]
    for in_bars, n, nulls in rows:
        label = "in the bars    " if in_bars else "not in the bars"
        pct = f"{100.0 * nulls / n:.2f}" if n else "n/a"
        lines.append(f"  {label} {n:>9}  null factor {nulls:>8}  {pct} percent")
    first, last = dal.coverage(dal.DAY_AGGS) or (None, None)
    lines.append(f"  bar coverage used: {first} .. {last}")
    return "\n".join(lines)


def report() -> str:
    sections = [
        ("Identifier: ticker to FIGI reuse", figi_reuse),
        ("Identifier: exposure after the liquidity screen", figi_exposure),
        ("Corporate actions: the ambiguous event key", corporate_action_duplicates),
        ("Dividends: the null adjustment factor", dividend_factor_nulls),
    ]
    out = []
    for title, fn in sections:
        try:
            body = fn()
        except Exception as exc:  # noqa: BLE001
            body = f"  not available: {exc}"
        out.append(f"=== {title} ===\n{body}")
    return "\n\n".join(out)


if __name__ == "__main__":
    print(f"sdp diagnostics, {dt.date.today()}\n")
    print(report())

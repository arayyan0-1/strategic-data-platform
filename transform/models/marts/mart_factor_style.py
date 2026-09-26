"""Style and industry factor returns: one weighted cross-sectional regression per session
on the exposures of mart_style_exposures (sdp.factors.cross_section_returns), as in a
Barra model. The design has one dummy per industry and the eight style z-scores. market
is the cap-weighted mean of the industry coefficients. Each industry return (ind_*) is
its coefficient less the market, so the cap-weighted industry returns sum to 0. Each
style return is the return of a portfolio with unit exposure to that style and none to
the other styles and the industries. The weight is the square root of the market cap.

A row is dated on the session that earns the return, from the close before. r2 is the
share of the cross-section of returns that the model explains on that session.
"""
import numpy as np

from sdp.factors import cross_section_returns, filled

STYLES = ("size", "liquidity", "beta", "momentum", "reversal", "volatility",
          "dividend_yield", "high_52w")
# The Fama-French 12 industries, and Unknown for a name with no SIC code yet.
INDUSTRIES = ("NoDur", "Durbl", "Manuf", "Enrgy", "Chems", "BusEq", "Telcm", "Utils",
              "Shops", "Hlth", "Money", "Other", "Unknown")


def _nulls(cols) -> str:
    """Return SQL that turns each NaN into a NULL, so an average downstream skips it."""
    return ", ".join(f"case when isnan(o.{c}) then null else o.{c} end as {c}" for c in cols)


def model(dbt, session):
    dbt.config(materialized="table")
    session.register("style_exposures", dbt.ref("mart_style_exposures"))
    session.register("style_signals", dbt.ref("mart_signals"))

    session.execute("""
        create or replace temp table style_days as
        select date, lead(date) over (order by date) as next_date,
               row_number() over (order by date) - 1 as day
        from (select distinct date from style_signals)""")
    codes = ", ".join(f"('{c}', {i})" for i, c in enumerate(INDUSTRIES))
    p = session.sql(f"""
        select d.day, coalesce(g.code, {len(INDUSTRIES) - 1}) as grp, e.cap, e.w, e.r,
               {", ".join(f"e.z_{s}" for s in STYLES)}
        from style_exposures e
        join style_days d on d.date = e.date
        left join (values {codes}) g(industry, code) on g.industry = e.industry
        where e.r is not null
        order by d.day, e.security_key""").fetchnumpy()

    Z = np.column_stack([filled(p[f"z_{s}"]) for s in STYLES])
    days, market, F, G, n, r2 = cross_section_returns(
        np.asarray(p["day"]), Z, filled(p["r"]), filled(p["w"]),
        groups=np.asarray(p["grp"]), n_groups=len(INDUSTRIES), cap=filled(p["cap"]))

    out = {"day": days.astype(np.int64), "n_names": n.astype(np.int64), "r2": r2,
           "market": market}
    for j, s in enumerate(STYLES):
        out[s] = F[:, j]
    for j, c in enumerate(INDUSTRIES):
        out[f"ind_{c.lower()}"] = G[:, j]
    session.register("style_arrays", out)
    session.execute("create or replace temp table style_out as select * from style_arrays")
    cols = ["r2", "market", *STYLES, *(f"ind_{c.lower()}" for c in INDUSTRIES)]
    session.execute(f"""
        create or replace temp table style_final as
        select d.next_date as date, o.n_names, {_nulls(cols)}
        from style_out o join style_days d using (day)
        where d.next_date is not null
        order by date""")
    return session.table("style_final")

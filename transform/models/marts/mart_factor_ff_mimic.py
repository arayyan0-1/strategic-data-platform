"""Fama-French factors mimicked with portfolios that trade in this universe
(sdp.factors.mimic_returns). The base assets are the style, industry and statistical
factor portfolios. Every 21 sessions, a ridge regression of each published factor on the base
returns over the trailing year gives the weights, and the weights earn the next 21
sessions out of sample. The library lags about two months, and the mimics carry each
factor to the last session. r2_<factor> is the in-sample fit of the latest weights."""
import numpy as np

from sdp.factors import filled, mimic_returns

STYLE_BASE = ("market", "size", "liquidity", "beta", "momentum", "reversal", "volatility",
              "dividend_yield", "high_52w", "ind_nodur", "ind_durbl", "ind_manuf",
              "ind_enrgy", "ind_chems", "ind_buseq", "ind_telcm", "ind_utils", "ind_shops",
              "ind_hlth", "ind_money", "ind_other")
PCA_BASE = ("pc1", "pc2", "pc3", "pc4", "pc5")
BASE = STYLE_BASE + PCA_BASE
TARGETS = ("mkt_rf", "smb", "hml", "rmw", "cma", "mom")


def _nulls(cols) -> str:
    """Return SQL that turns each NaN into a NULL, so an average downstream skips it."""
    return ", ".join(f"case when isnan(o.{c}) then null else o.{c} end as {c}" for c in cols)


def model(dbt, session):
    dbt.config(materialized="table")
    session.register("mimic_style", dbt.ref("mart_factor_style"))
    session.register("mimic_pca", dbt.ref("mart_factor_pca"))
    session.register("mimic_ff", dbt.ref("stg_french_factors"))

    session.execute("""
        create or replace temp table mimic_days as
        select date, row_number() over (order by date) - 1 as day
        from mimic_style""")
    panel = session.sql(f"""
        select {", ".join(f"s.{c}" for c in STYLE_BASE)},
               {", ".join(f"p.{c}" for c in PCA_BASE)},
               {", ".join(f"f.{c}" for c in TARGETS)}
        from mimic_days d
        join mimic_style s using (date)
        left join mimic_pca p using (date)
        left join mimic_ff f using (date)
        order by d.day""").fetchnumpy()

    B = np.column_stack([filled(panel[c]) for c in BASE])
    out = {"day": np.arange(B.shape[0], dtype=np.int64)}
    fits = {}
    for t in TARGETS:
        out[t], fits[f"r2_{t}"] = mimic_returns(filled(panel[t]), B)
    out.update(fits)

    session.register("mimic_arrays", out)
    session.execute("create or replace temp table mimic_out as select * from mimic_arrays")
    session.execute(f"""
        create or replace temp table mimic_final as
        select d.date, {_nulls([*TARGETS, *(f"r2_{t}" for t in TARGETS)])}
        from mimic_out o join mimic_days d using (day)
        where not isnan(o.mkt_rf)
        order by date""")
    return session.table("mimic_final")

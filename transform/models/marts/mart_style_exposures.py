"""The exposures of each in-universe name on each session: its industry, its z-score on
each style (sdp.factors.standardize) and the return of the next session. The styles are
the characteristics of a Barra model that daily prices and the monthly ticker details
give: size (log market cap), liquidity (log dollar volume over market cap), beta,
momentum, reversal, volatility, dividend yield and the 52-week high.

The regression weight is the square root of the market cap. A name with no market cap
takes the median ratio of cap to dollar volume of its session, so it keeps a weight and
its size z-score is 0. mart_factor_style and mart_residuals read this table.
"""
import numpy as np

from sdp.factors import filled, standardize

STYLES = {
    "size": "ln(u.market_cap)",
    "liquidity": "ln(u.adv / u.market_cap)",
    "beta": "s.beta_252",
    "momentum": "s.momentum_12_1",
    "reversal": "s.reversal_5",
    "volatility": "s.vol_60",
    "dividend_yield": "s.div_yield",
    "high_52w": "s.hi_52w",
}


def model(dbt, session):
    dbt.config(materialized="table")
    session.register("exp_signals", dbt.ref("mart_signals"))
    session.register("exp_forward", dbt.ref("mart_forward_returns"))
    session.register("exp_universe", dbt.ref("stg_universe"))

    session.execute(f"""
        create or replace temp table exp_panel as
        select
            row_number() over (order by s.date, s.security_key) - 1  as rid,
            dense_rank() over (order by s.date) - 1                  as day,
            s.security_key, s.ticker, s.date, u.industry,
            u.market_cap, u.adv,
            {", ".join(f"{expr} as x_{name}" for name, expr in STYLES.items())},
            f.fwd_ret_1 as r
        from exp_signals s
        join exp_universe u on u.ticker = s.ticker and u.date = s.date
        left join exp_forward f on f.security_key = s.security_key and f.date = s.date
        where s.in_universe and u.adv > 0""")
    p = session.sql(f"""
        select day, market_cap, adv, {", ".join(f"x_{n}" for n in STYLES)}
        from exp_panel order by rid""").fetchnumpy()

    day = np.asarray(p["day"])
    cap, adv = filled(p["market_cap"]), filled(p["adv"])
    # A missing cap takes the median cap per dollar of volume of its session.
    ratio = np.where(np.isfinite(cap) & (cap > 0), cap / adv, np.nan)
    order = np.argsort(day, kind="stable")
    _, starts = np.unique(day[order], return_index=True)
    ends = np.append(starts[1:], day.size)
    cap_filled = cap.copy()
    for a, b in zip(starts, ends, strict=True):
        rows = order[a:b]
        med = np.nanmedian(ratio[rows]) if np.isfinite(ratio[rows]).any() else 1.0
        gap = ~(np.isfinite(cap[rows]) & (cap[rows] > 0))
        cap_filled[rows[gap]] = adv[rows[gap]] * med
    w = np.sqrt(cap_filled)

    X = np.column_stack([filled(p[f"x_{n}"]) for n in STYLES])
    X[~np.isfinite(X)] = np.nan
    Z = standardize(day, X, w)

    out = {"rid": np.arange(day.size, dtype=np.int64), "cap": cap_filled, "w": w}
    for j, n in enumerate(STYLES):
        out[f"z_{n}"] = Z[:, j]
    session.register("exp_arrays", out)
    session.execute("""
        create or replace temp table exp_final as
        select e.security_key, e.ticker, e.date, e.industry, e.market_cap, a.cap, a.w,
               a.* exclude (rid, cap, w), e.r
        from exp_panel e join exp_arrays a using (rid)
        order by e.date, e.security_key""")
    return session.table("exp_final")

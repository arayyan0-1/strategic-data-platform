"""The exposures of each in-universe name on each session: its industry, its z-score on
each style (sdp.factors.standardize) and the return of the next session. The styles are
the characteristics of a Barra model that daily prices and the monthly ticker details
give: size (log market cap), liquidity (log dollar volume over market cap), beta,
momentum, reversal, volatility, dividend yield and the 52-week high.

The regression weight is the square root of the market cap. A name with no market cap
takes the median ratio of cap to dollar volume of its session, so it keeps a weight and
its size z-score is 0. The var style_center sets the mean of each z-score: sqrt_cap (the
regression weight) or cap (as in Barra, so the cap-weighted market has no style
exposure). style_clip is the largest z-score. style_factor_returns and style_residuals
read this table.
"""
import numpy as np

from sdp.factors import STYLE_SQL, filled, stack, standardize

STYLES = STYLE_SQL


def model(dbt, session):
    dbt.config(materialized="table")
    center = dbt.config.get("style_center")
    clip = float(dbt.config.get("style_clip"))
    if center not in ("sqrt_cap", "cap"):
        raise ValueError(f"style_center must be sqrt_cap or cap. It is {center}.")
    session.register("exp_signals", dbt.ref("signals"))
    session.register("exp_forward", dbt.ref("forward_returns"))
    session.register("exp_universe", dbt.ref("int_universe"))

    session.execute(f"""
        create or replace temp table exp_panel as
        select
            s.date - date '1970-01-01'                               as day,
            s.security_key, s.ticker, s.date, u.industry,
            u.market_cap, u.adv,
            {", ".join(f"{expr} as x_{name}" for name, expr in STYLES.items())},
            f.fwd_ret_1 as r
        from exp_signals s
        join exp_universe u on u.ticker = s.ticker and u.date = s.date
        left join exp_forward f on f.security_key = s.security_key and f.date = s.date
        where s.in_universe and u.adv > 0""")
    p = session.sql(f"""
        select rowid as rid, day, market_cap, adv, {", ".join(f"x_{n}" for n in STYLES)}
        from exp_panel""").fetchnumpy()

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

    X = stack(p[f"x_{n}"] for n in STYLES)
    Z = standardize(day, X, cap_filled if center == "cap" else w, clip)

    out = {"rid": np.asarray(p["rid"], dtype=np.int64), "cap": cap_filled, "w": w}
    for j, n in enumerate(STYLES):
        out[f"z_{n}"] = Z[:, j]
    session.register("exp_arrays", out)
    return session.sql("""
        select e.security_key, e.ticker, e.date, e.industry, e.market_cap,
               a.cap as weight_cap, a.w as weight, a.* exclude (rid, cap, w),
               e.r as fwd_ret_1
        from exp_panel e join exp_arrays a on a.rid = e.rowid
        order by e.date, e.security_key""")

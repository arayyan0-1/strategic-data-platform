"""The exposures of the common stock that the estimation universe leaves out: the industry and
the eight style z-scores of each such name per session, and the return of the next session.
A name is in the coverage set when its type is in coverage_types, its close is above 0 and it
is not in the universe.

The fit never sees these names. Each z-score uses the parameters of the universe on the same
session, which style_exposures used: the winsor bounds, the mean and the standard deviation
of the universe values. A coverage name thus sits on the scale of the universe. A missing
characteristic gives 0. The return of the next session is null when the name has no bar on
the next session, because the return of a longer gap does not belong to the factor returns
of one session. The var coverage_sessions limits the build to the latest sessions. A session
with no universe has no parameters, so it has no row.
"""
import json

import numpy as np

from sdp.factors import STYLE_SQL, filled, standardize_apply, standardize_fit


def model(dbt, session):
    dbt.config(materialized="table")
    center = dbt.config.get("style_center")
    clip = float(dbt.config.get("style_clip"))
    n_sessions = int(dbt.config.get("coverage_sessions"))
    types = dbt.config.get("coverage_types")
    if isinstance(types, str):
        types = json.loads(types)
    if center not in ("sqrt_cap", "cap"):
        raise ValueError(f"style_center must be sqrt_cap or cap. It is {center}.")
    if not (isinstance(types, list) and types and all(isinstance(t, str) for t in types)):
        raise ValueError(f"coverage_types must be a non-empty list of types. It is {types}.")
    if n_sessions < 0:
        raise ValueError(f"coverage_sessions must be 0 or more. It is {n_sessions}.")
    session.register("cov_signals", dbt.ref("signals"))
    session.register("cov_forward", dbt.ref("forward_returns"))
    session.register("cov_universe", dbt.ref("int_universe"))
    session.register("cov_style", dbt.ref("style_exposures"))

    first = ("(select min(date) from (select distinct date from cov_signals "
             f"order by date desc limit {n_sessions}))") if n_sessions else "date '1900-01-01'"
    weight = "weight_cap" if center == "cap" else "weight"
    listed = ", ".join("'" + t.replace("'", "''") + "'" for t in types)
    session.execute(f"""
        create or replace temp table cov_panel as
        with days as (
            select date, lead(date) over (order by date) as next_date
            from (select distinct date from cov_signals)
        ), bars as (
            select security_key, date,
                   lead(date) over (partition by security_key order by date) as bar_next
            from cov_signals
            where date >= {first}
        )
        select
            row_number() over (order by s.date, s.security_key) - 1  as rid,
            dense_rank() over (order by s.date) - 1                  as day,
            s.security_key, s.ticker, s.date, u.type_filled, u.industry, u.market_cap,
            not s.in_universe                                        as is_cov,
            e.{weight}                                               as w,
            {", ".join(f"{expr} as x_{name}" for name, expr in STYLE_SQL.items())},
            case when b.bar_next = d.next_date then f.fwd_ret_1 end  as r
        from cov_signals s
        join cov_universe u on u.ticker = s.ticker and u.date = s.date
        join bars b on b.security_key = s.security_key and b.date = s.date
        join days d on d.date = s.date
        left join cov_forward f on f.security_key = s.security_key and f.date = s.date
        left join cov_style e on e.security_key = s.security_key and e.date = s.date
        where (s.in_universe and u.adv > 0)
           or (not s.in_universe and u.type_filled in ({listed}) and u.close > 0)""")
    p = session.sql(f"""
        select is_cov, day, w, {", ".join(f"x_{n}" for n in STYLE_SQL)}
        from cov_panel order by rid""").fetchnumpy()

    cov = np.asarray(p["is_cov"], dtype=bool)
    day, w = np.asarray(p["day"]), filled(p["w"])
    if not np.isfinite(w[~cov]).all():
        raise ValueError("A universe name has no regression weight in style_exposures.")
    X = np.column_stack([filled(p[f"x_{n}"]) for n in STYLE_SQL])
    X[~np.isfinite(X)] = np.nan
    days, params = standardize_fit(day[~cov], X[~cov], w[~cov])
    Z, known = standardize_apply(day[cov], X[cov], days, params, clip)

    rid = np.flatnonzero(cov).astype(np.int64)
    out = {"rid": rid[known]}
    for j, n in enumerate(STYLE_SQL):
        out[f"z_{n}"] = Z[known, j]
    session.register("cov_arrays", out)
    session.execute("""
        create or replace temp table cov_final as
        select e.security_key, e.ticker, e.date, e.type_filled, e.industry, e.market_cap,
               a.* exclude (rid), e.r as fwd_ret_1
        from cov_panel e join cov_arrays a using (rid)
        order by e.date, e.security_key""")
    return session.table("cov_final")

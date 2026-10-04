"""The exposures of each liquid fund to the factors of the style model and to a block of
macro factors. A fund is not a company, so its size or its dividend yield means nothing,
and its return is a weighted sum of names that the style model already holds. Instead,
every fund_step sessions, a ridge regression of the daily return of the fund on the factor
returns over the fund_window sessions before gives one loading for each factor
(sdp.factors.fund_loadings). A factor return is the return of a portfolio with unit
exposure, so a loading is in the units of the z-score of a stock.

A row is one fit of one fund. The loadings earn the sessions from fit_date to last_date.
window_end is the last session that the fit reads. It is the session before fit_date.
intercept is the daily constant of the fit. It is not a factor part. r2_in is the share of
the variance of the returns in the window that the fit explains.

The fund lines come from int_fund_returns. A fund gets a fit when its trailing dollar volume
is at least fund_min_adv on fit_date and it has at least fund_min_returns returns before
fit_date. At least fund_min_obs rows of the window must also have its return and every
factor. The vars fund_window, fund_step, fund_ridge and fund_macro set the window, the step,
the penalty and the macro factors.
"""
import numpy as np

from sdp.factors import filled, fund_loadings

STYLES = ("size", "liquidity", "beta", "momentum", "reversal", "volatility",
          "dividend_yield", "high_52w")
INDUSTRIES = ("NoDur", "Durbl", "Manuf", "Enrgy", "Chems", "BusEq", "Telcm", "Utils",
              "Shops", "Hlth", "Money", "Other", "Unknown")
MODEL = ("market", *STYLES, *(f"ind_{c.lower()}" for c in INDUSTRIES))
MACRO = ("d_t_10y", "d_t_2y", "d_curve_10y_2y", "d_real_10y", "d_hy_oas", "d_dollar",
         "d_wti", "d_bitcoin", "d_usdjpy", "d_eurusd")


def model(dbt, session):
    dbt.config(materialized="table")
    macro = [c.strip() for c in str(dbt.config.get("fund_macro")).split(",") if c.strip()]
    unknown = [c for c in macro if c not in MACRO]
    if unknown:
        raise ValueError(f"fund_macro holds names that are not macro factors: {unknown}. "
                         f"The macro factors are {list(MACRO)}.")
    window = int(dbt.config.get("fund_window"))
    step = int(dbt.config.get("fund_step"))
    min_obs = int(dbt.config.get("fund_min_obs"))
    min_returns = int(dbt.config.get("fund_min_returns"))
    min_adv = float(dbt.config.get("fund_min_adv"))
    penalty = float(dbt.config.get("fund_ridge"))
    if min(window, step, min_obs, min_returns) < 1 or penalty < 0:
        raise ValueError("fund_window, fund_step, fund_min_obs and fund_min_returns must be 1 "
                         "or more, and fund_ridge must be 0 or more.")
    names = [*MODEL, *macro]
    session.register("fund_style", dbt.ref("style_factor_returns"))
    session.register("fund_macro", dbt.ref("macro_factor_returns"))
    session.register("fund_returns", dbt.ref("int_fund_returns"))

    f = session.sql(f"""
        select s.date, {", ".join(f"s.{c}" for c in MODEL)}
               {"".join(f", m.{c}" for c in macro)}
        from fund_style s left join fund_macro m using (date)
        order by s.date""").fetchnumpy()
    dates = np.asarray(f["date"]).astype("datetime64[D]")
    X = np.column_stack([filled(f[c]) for c in names])

    session.execute(f"""
        create or replace temp table fund_lines as
        with lines as materialized (
            select security_key, ticker, date, ret, adv from fund_returns
        )
        select security_key, ticker, date, ret, adv
        from lines
        where security_key in (
            select security_key from lines
            group by security_key
            having max(adv) >= {min_adv} and count(ret) >= {min_returns})""")
    # The code of a fund is its rank in the sorted keys, as np.unique would give it.
    q = session.sql("""
        select dense_rank() over (order by security_key) - 1 as col, date, ret, adv
        from fund_lines""").fetchnumpy()
    keys = session.sql(
        "select distinct security_key from fund_lines order by security_key").fetchnumpy()
    keys, col = np.asarray(keys["security_key"], dtype=str), np.asarray(q["col"])
    qdate = np.asarray(q["date"]).astype("datetime64[D]")
    day = np.minimum(np.searchsorted(dates, qdate), dates.size - 1)
    inside = dates[day] == qdate
    R = np.full((dates.size, keys.size), np.nan)
    A = np.full((dates.size, keys.size), np.nan)
    R[day[inside], col[inside]] = filled(q["ret"])[inside]
    A[day[inside], col[inside]] = filled(q["adv"])[inside]

    rows, B, alpha, r2, n_obs = fund_loadings(
        R, X, np.nan_to_num(A) >= min_adv, window=window, step=step, min_obs=min_obs,
        min_returns=min_returns, penalty=penalty)
    i, j = np.nonzero(np.isfinite(B[:, :, 0]))
    first = rows[i]
    out = {
        "security_key": keys[j].astype(object),
        "fit_date": dates[first].astype("datetime64[us]"),
        "window_end": dates[first - 1].astype("datetime64[us]"),
        "last_date": dates[np.minimum(first + step, dates.size) - 1].astype("datetime64[us]"),
        "r2_in": r2[i, j],
        "n_obs": n_obs[i, j].astype(np.int64),
        "ridge": np.full(i.size, penalty),
        "intercept": alpha[i, j],
    }
    for k, c in enumerate(names):
        out[c] = B[i, j, k]
    session.register("fund_arrays", out)
    session.execute("create or replace temp table fund_out as select * from fund_arrays")
    session.execute(f"""
        create or replace temp table fund_final as
        select e.security_key, t.ticker, e.fit_date::date as fit_date,
               e.window_end::date as window_end, e.last_date::date as last_date,
               case when isnan(e.r2_in) then null else e.r2_in end as r2_in,
               e.n_obs, e.ridge, e.intercept, {", ".join(f"e.{c}" for c in names)}
        from fund_out e
        asof left join fund_lines t
            on t.security_key = e.security_key and e.window_end >= t.date
        order by e.security_key, e.fit_date""")
    return session.table("fund_final")

"""Cross-sectional IC and tercile spread of mom, val and their combination, by universe."""
import numpy as np

from sdp.evaluation import newey_west

LAG = 5
MIN_NAMES = 5
UNIVERSES = ("regions", "countries", "us_segments")
SIGNALS = ("mom", "val", "combo")

# One row per universe, signal and month. A signal uses the names that have it and a next-month
# return, so mom has a longer sample than val and combo. combo uses the names with both, scored
# against the mean and deviation of those names. Ranks are averages for ties. A tercile is a
# third of the names by signal; the spread is the top third less the bottom third, equal weight.
MONTHLY = """
    with base as (
        select universe, index_key, month, mom, val, fwd_ret
        from sig_history
        where universe in ('regions', 'countries', 'us_segments') and fwd_ret is not null
    ), both_signals as (
        select *, avg(mom) over w as mom_mean, stddev_samp(mom) over w as mom_sd,
               avg(val) over w as val_mean, stddev_samp(val) over w as val_sd
        from base
        where mom is not null and val is not null
        window w as (partition by universe, month)
    ), long as (
        select universe, index_key, month, fwd_ret, 'mom' as signal, mom as value
        from base where mom is not null
        union all
        select universe, index_key, month, fwd_ret, 'val', val
        from base where val is not null
        union all
        select universe, index_key, month, fwd_ret, 'combo',
               ((mom - mom_mean) / mom_sd + (val - val_mean) / val_sd) / 2
        from both_signals
        where mom_sd > 0 and val_sd > 0
    ), counted as (
        select *, count(*) over (partition by universe, signal, month) as n
        from long
    ), r as (
        select *,
               rank() over (partition by universe, signal, month order by value)
                   + (count(*) over (partition by universe, signal, month, value) - 1) / 2.0
                   as value_rank,
               rank() over (partition by universe, signal, month order by fwd_ret)
                   + (count(*) over (partition by universe, signal, month, fwd_ret) - 1) / 2.0
                   as ret_rank,
               ntile(3) over (partition by universe, signal, month order by value) as tercile
        from counted
        where n >= {min_names}
    )
    select universe, signal, month, count(*) as n_names, corr(value_rank, ret_rank) as ic,
           avg(fwd_ret) filter (where tercile = 3) - avg(fwd_ret) filter (where tercile = 1)
               as spread
    from r
    group by universe, signal, month
    order by universe, signal, month"""

COLUMNS = ("universe", "signal", "period", "first_month", "last_month", "n_months", "mean_names",
           "mean_ic", "t_nw", "spread_ann", "spread_t_nw")


def _t(x: np.ndarray) -> tuple[float, float]:
    """Return the mean of x and its Newey-West t."""
    mean, se = newey_west(x, LAG)
    return mean, (mean / se if se > 0 else np.nan)


def model(dbt, session):
    dbt.config(materialized="table")
    dev_end = np.datetime64(str(dbt.config.get("fund_signal_dev_end")), "D")
    session.register("sig_history", dbt.ref("fund_signal_history"))
    m = session.sql(MONTHLY.format(min_names=MIN_NAMES)).fetchnumpy()
    universe = np.asarray(m["universe"], dtype=object)
    signal = np.asarray(m["signal"], dtype=object)
    month = np.asarray(m["month"]).astype("datetime64[D]")
    ic = np.ma.filled(np.ma.asarray(m["ic"], dtype=float), np.nan)
    spread = np.ma.filled(np.ma.asarray(m["spread"], dtype=float), np.nan)
    n_names = np.asarray(m["n_names"], dtype=float)

    out: dict[str, list] = {c: [] for c in COLUMNS}
    for u in UNIVERSES:
        for s in SIGNALS:
            group = (universe == u) & (signal == s)
            for period, use in (("development", group & (month <= dev_end)), ("all", group)):
                mean_ic, t_ic = _t(ic[use])
                mean_sp, t_sp = _t(spread[use])
                row = (u, s, period,
                       month[use].min() if use.any() else np.datetime64("NaT"),
                       month[use].max() if use.any() else np.datetime64("NaT"),
                       int(np.isfinite(ic[use]).sum()),
                       n_names[use].mean() if use.any() else np.nan,
                       mean_ic, t_ic, mean_sp * 12, t_sp)
                for c, v in zip(COLUMNS, row, strict=True):
                    out[c].append(v)
    arrays = {
        "universe": np.array(out["universe"], dtype=object),
        "signal": np.array(out["signal"], dtype=object),
        "period": np.array(out["period"], dtype=object),
        "first_month": np.array(out["first_month"], dtype="datetime64[us]"),
        "last_month": np.array(out["last_month"], dtype="datetime64[us]"),
        "n_months": np.array(out["n_months"], dtype=np.int64),
        **{c: np.array(out[c], dtype=float) for c in COLUMNS[6:]},
    }
    session.register("sig_ic_arrays", arrays)
    session.execute("""
        create or replace temp table sig_ic_final as
        select universe, signal, period, first_month::date as first_month,
               last_month::date as last_month, n_months, mean_names,
               case when isnan(mean_ic) then null else mean_ic end as mean_ic,
               case when isnan(t_nw) then null else t_nw end as t_nw,
               case when isnan(spread_ann) then null else spread_ann end as spread_ann,
               case when isnan(spread_t_nw) then null else spread_t_nw end as spread_t_nw
        from sig_ic_arrays""")
    return session.table("sig_ic_final")

{#
  Mean IC, its volatility, session count and naive t, per signal and horizon.
  The t assumes independent daily ICs. Where ic_autocorr_lag1 is large the
  forward windows overlap, so correct it with a Newey-West standard error.
#}

with d as (

    select
        signal, horizon, ic,
        lag(ic) over (partition by signal, horizon order by date) as lag_ic
    from {{ ref('mart_signal_ic') }}

)

select
    signal,
    horizon,
    count(*)                                                 as n_days,
    avg(ic)                                                  as mean_ic,
    stddev_samp(ic)                                          as ic_std,
    avg(ic) / nullif(stddev_samp(ic) / sqrt(count(*)), 0)    as t_stat,
    corr(ic, lag_ic)                                         as ic_autocorr_lag1
from d
group by signal, horizon
order by signal, horizon

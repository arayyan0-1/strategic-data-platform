{#
  The daily cross-sectional rank correlation (Spearman IC) between each signal
  at D and the forward return over each horizon, over in_universe names.
#}

with sig as (

    select security_key, date, signal, value
    from (
        unpivot {{ ref('mart_signals') }}
        on {{ signal_columns() }}
        into name signal value value
    )
    where in_universe

), fwd as (

    select security_key, date, horizon, fwd_ret
    from (
        unpivot {{ ref('mart_forward_returns') }}
        on fwd_ret_1, fwd_ret_5, fwd_ret_21
        into name horizon value fwd_ret
    )

), joined as (

    select s.date, s.signal, f.horizon, s.value, f.fwd_ret
    from sig s
    inner join fwd f
        on f.security_key = s.security_key and f.date = s.date
    where s.value is not null and f.fwd_ret is not null

), ranked as (

    select
        date, signal, horizon,
        rank() over (partition by date, signal, horizon order by value)   as r_value,
        rank() over (partition by date, signal, horizon order by fwd_ret) as r_fwd
    from joined

)

select
    date, signal, horizon,
    corr(r_value, r_fwd) as ic,
    count(*)             as n
from ranked
group by date, signal, horizon
having count(*) >= {{ var('ic_min_names') }}

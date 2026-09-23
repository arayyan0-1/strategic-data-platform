{#
  The average cross-sectional rank correlation between each pair of signals. A
  pair near one is one bet under two names. Upper triangle and diagonal only.
#}

with r as (

    select
        date, security_key, signal,
        rank() over (partition by date, signal order by value) as rk
    from (
        unpivot {{ ref('mart_signals') }}
        on {{ signal_columns() }}
        into name signal value value
    )
    where in_universe and value is not null

), per_day as (

    select
        a.date,
        a.signal as signal_a,
        b.signal as signal_b,
        corr(a.rk, b.rk) as c
    from r a
    inner join r b
        on b.date = a.date and b.security_key = a.security_key
       and a.signal <= b.signal
    group by a.date, a.signal, b.signal

)

select
    signal_a,
    signal_b,
    avg(c)   as rank_corr,
    count(*) as n_days
from per_day
group by signal_a, signal_b
order by signal_a, signal_b

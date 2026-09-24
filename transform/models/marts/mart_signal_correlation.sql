{#
  The average cross-sectional rank correlation between each pair of signals. A
  pair near one is one bet under two names. Upper triangle and diagonal only.
  The rank is the panel avg_rank. Tied values get the average of the ranks that
  they occupy.
#}

with r as (

    select date, security_key, signal, avg_rank as rk
    from {{ ref('mart_signal_panel') }}

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

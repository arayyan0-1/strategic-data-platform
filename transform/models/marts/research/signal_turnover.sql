{# Fraction of the top signal quintile replaced each session, averaged over sessions. The top is
   the highest panel quintile that has names. Read beside signal_long_short. #}

with top as (

    select signal, date, security_key
    from {{ ref('signal_panel') }}
    qualify quintile = max(quintile) over (partition by date, signal)

), seq as (

    select
        signal, date, security_key,
        dense_rank() over (partition by signal order by date) as rn
    from top

), retained as (

    select
        c.signal, c.date,
        case when p.security_key is not null then 1 else 0 end as held
    from seq c
    left join seq p
        on p.signal = c.signal and p.rn = c.rn - 1
       and p.security_key = c.security_key
    where c.rn > 1

), daily as (

    -- One fraction per session, so a large tied top bin gets no extra weight.
    select signal, date, avg(1.0 - held) as turnover
    from retained
    group by signal, date

)

select
    signal,
    avg(turnover) as avg_turnover,
    count(*)      as n_days
from daily
group by signal
order by signal

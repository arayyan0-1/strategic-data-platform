{#
  The fraction of the top signal quintile replaced each session, averaged over
  sessions. A strong IC with a high turnover can still be uninvestable, because
  cost erodes it. Read it beside mart_signal_ls_returns. The top is the highest
  panel quintile that has names in the session. Tied values share one bin.
#}

with top as (

    select signal, date, security_key
    from {{ ref('mart_signal_panel') }}
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

    -- One fraction per session, so a session with a large tied top bin does not
    -- get more weight in the average.
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

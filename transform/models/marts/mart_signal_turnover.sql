{#
  The fraction of the top signal quintile replaced each session, averaged over
  sessions. A strong IC with a high turnover can still be uninvestable, because
  cost erodes it. Read it beside mart_signal_ls_returns.
#}

with sig as (

    select security_key, date, signal, value
    from (
        unpivot {{ ref('mart_signals') }}
        on {{ signal_columns() }}
        into name signal value value
    )
    where in_universe and value is not null

), top as (

    select signal, date, security_key
    from (
        select
            signal, date, security_key,
            ntile(5) over (partition by date, signal order by value) as quintile
        from sig
    )
    where quintile = 5

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

)

select
    signal,
    avg(1.0 - held)   as avg_turnover,
    count(distinct date) as n_days
from retained
group by signal
order by signal

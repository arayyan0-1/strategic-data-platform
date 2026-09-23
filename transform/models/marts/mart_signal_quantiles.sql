{#
  Signal shape. Each session, in_universe names are sorted into five groups by
  the signal, and the forward return is averaged in each group over all
  sessions. A monotone rise from group 1 to 5 is the signal at work.
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

    select security_key, date, fwd_ret_1, fwd_ret_5, fwd_ret_21
    from {{ ref('mart_forward_returns') }}

), joined as (

    select
        s.date, s.signal, s.value,
        f.fwd_ret_1, f.fwd_ret_5, f.fwd_ret_21
    from sig s
    inner join fwd f on f.security_key = s.security_key and f.date = s.date
    where s.value is not null

), binned as (

    select
        *,
        ntile(5) over (partition by date, signal order by value) as quintile
    from joined

)

select
    signal,
    quintile,
    count(*)          as n,
    avg(fwd_ret_1)    as mean_fwd_1,
    avg(fwd_ret_5)    as mean_fwd_5,
    avg(fwd_ret_21)   as mean_fwd_21
from binned
group by signal, quintile
order by signal, quintile

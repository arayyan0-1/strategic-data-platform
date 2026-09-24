{#
  The signal panel in long form. One row per in_universe name, session and
  signal with a value, and the forward returns of that name. The marts that
  evaluate a signal read this panel. avg_rank is the rank of the value in its
  session and signal. Tied values get the average of the ranks that they
  occupy. quintile comes from avg_rank, so tied values share one bin and a
  rebuild gives the same bins.
#}

with sig as (

    select date, security_key, ticker, signal, value
    from (
        unpivot {{ ref('mart_signals') }}
        on {{ signal_columns() }}
        into name signal value value
    )
    where in_universe and value is not null

), ranked as (

    -- rank() is 1 more than the count of lower values. count(*) over w is the
    -- count of values at or below this value. Their mean is the average rank.
    select
        *,
        count(*) over (partition by date, signal)   as n,
        (rank() over w + count(*) over w) / 2.0     as avg_rank
    from sig
    window w as (
        partition by date, signal order by value
        range between unbounded preceding and current row)

)

select
    r.date,
    r.security_key,
    r.ticker,
    r.signal,
    r.value,
    r.n,
    r.avg_rank,
    cast(ceil(r.avg_rank * 5.0 / r.n) as integer) as quintile,
    f.fwd_ret_1,
    f.fwd_ret_5,
    f.fwd_ret_21
from ranked r
left join {{ ref('mart_forward_returns') }} f
    on f.security_key = r.security_key and f.date = r.date

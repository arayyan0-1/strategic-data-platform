{#
  The signal panel in long form. One row per in_universe name, session and
  signal with a value, and the forward returns of that name. The marts that
  evaluate a signal read this panel. avg_rank is the rank of the value in its
  session and signal. Tied values get the average of the ranks that they
  occupy. quintile comes from avg_rank, so tied values share one bin and a
  rebuild gives the same bins.
#}

with sig as (

    -- The forward returns join before the unpivot, when there is one row for
    -- each name and session.
    select date, security_key, ticker, signal, value, fwd_ret_1, fwd_ret_5, fwd_ret_21
    from (
        unpivot (
            select s.*, f.fwd_ret_1, f.fwd_ret_5, f.fwd_ret_21
            from {{ ref('mart_signals') }} s
            left join {{ ref('mart_forward_returns') }} f
                on f.security_key = s.security_key and f.date = s.date
            where s.in_universe
        )
        on {{ signal_columns() }}
        into name signal value value
    )
    where value is not null

), ranked as (

    -- rank() is 1 more than the count of lower values. count(*) over w is the
    -- count of values at or below this value. Their mean is the average rank.
    -- Both windows sort on the same keys, so they share one sort.
    select
        *,
        count(*) over (
            partition by date, sid order by value
            rows between unbounded preceding and unbounded following) as n,
        (rank() over w + count(*) over w) / 2.0                       as avg_rank
    from (select *, {{ signal_id('signal') }} as sid from sig)
    window w as (
        partition by date, sid order by value
        range between unbounded preceding and current row)

)

select
    date,
    security_key,
    ticker,
    signal,
    value,
    n,
    avg_rank,
    cast(ceil(avg_rank * 5.0 / n) as integer) as quintile,
    fwd_ret_1,
    fwd_ret_5,
    fwd_ret_21
from ranked

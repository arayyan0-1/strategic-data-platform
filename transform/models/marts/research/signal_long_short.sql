{#
  Long-short return of each signal: the mean forward return of the top quintile less
  the mean of the bottom quintile. Each leg is equal weight, rebalanced daily, and
  gross of cost. ret keeps every return as printed, so it holds the squeezes and the
  jumps that a portfolio earns. cum_ret is the arithmetic sum of ret. Read it beside
  signal_turnover.

  The bins are the panel quintiles, so tied values share one bin. Many tied values can
  leave bin 1 or bin 5 empty, so each side is the lowest or highest bin that has names.

  ret_winsorized is a robustness check. It clips each forward return to the
  long_short_winsor and 1 - long_short_winsor quantiles of its session before it
  averages the legs. A portfolio does not earn that return.
#}

with bins as (

    -- The top and bottom bins come from all names in the session, as in
    -- signal_turnover.
    select
        signal, date, quintile, fwd_ret_1,
        min(quintile) over (partition by date, signal) as bottom,
        max(quintile) over (partition by date, signal) as top
    from {{ ref('signal_panel') }}

), binned as (

    select * from bins
    where fwd_ret_1 is not null

), caps as (

    select
        signal, date,
        quantile_cont(fwd_ret_1, {{ var('long_short_winsor') }})     as lo,
        quantile_cont(fwd_ret_1, 1 - {{ var('long_short_winsor') }}) as hi
    from binned
    group by signal, date

), capped as (

    select
        b.signal, b.date, b.quintile, b.bottom, b.top, b.fwd_ret_1,
        least(greatest(b.fwd_ret_1, c.lo), c.hi) as w_ret
    from binned b
    inner join caps c on c.signal = b.signal and c.date = b.date

), daily as (

    -- A session where all names share one bin has no short side, so ret is null.
    select
        signal,
        date,
        avg(fwd_ret_1) filter (where quintile = top)
            - avg(fwd_ret_1) filter (where quintile = bottom and bottom < top) as ret,
        avg(w_ret) filter (where quintile = top)
            - avg(w_ret) filter (where quintile = bottom and bottom < top)     as ret_winsorized
    from capped
    group by signal, date

)

select
    signal,
    date,
    ret,
    sum(ret) over (
        partition by signal order by date
        rows between unbounded preceding and current row) as cum_ret,
    ret_winsorized
from daily
